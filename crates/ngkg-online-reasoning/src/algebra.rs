//! Standards-preserving substitution of exact OWL Direct BGP answers into SPARQL algebra.
//!
//! SPARQL entailment regimes redefine BGP matching. The surrounding SPARQL algebra still owns
//! joins, left joins, union, minus, projection, grouping, ordering, slicing and query-form
//! construction. Replacing each admitted BGP with its exact solution multiset therefore lets the
//! already-qualified scalar evaluator apply every outer operator without reimplementing it here.

use ngkg_types::{
    DirectBgpCompleteness, DirectBgpExactness, DirectBgpRdfTerm, DirectBgpResult, DirectBgpStatus,
    validate_direct_bgp_result,
};
use spargebra::{
    Query,
    algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression},
    term::{GroundTerm, Literal, NamedNode, Variable},
};
use thiserror::Error;

/// Fail-closed exact-algebra substitution error.
#[derive(Debug, Error)]
pub enum ExactAlgebraError {
    /// A BGP result is missing, duplicated, out of order, or not exact and complete.
    #[error("exact BGP result set does not match the parsed SPARQL algebra")]
    ResultSet,
    /// A reasoner binding cannot be represented as a SPARQL VALUES ground term.
    #[error("exact BGP result contains an invalid or unsupported RDF term")]
    Term,
    /// Expanding compressed bag multiplicity would exceed the admitted result ceiling.
    #[error("exact BGP solution multiset exceeds its substitution row ceiling")]
    RowCeiling,
}

/// Replace every BGP, including BGPs nested in `EXISTS`, with its exact solution multiset.
///
/// `results` must be in the same depth-first ordinal order produced by `ngkg-owl-direct`.
/// Empty exact relations become an empty `VALUES` table; compressed multiplicities are expanded
/// exactly because SPARQL joins operate over multisets rather than sets.
pub fn substitute_exact_bgp_results(
    query: Query,
    results: &[DirectBgpResult],
    max_expanded_rows: usize,
) -> Result<Query, ExactAlgebraError> {
    if max_expanded_rows == 0 {
        return Err(ExactAlgebraError::RowCeiling);
    }
    let mut cursor = ResultCursor {
        results,
        ordinal: 0,
        expanded_rows: 0,
        max_expanded_rows,
    };
    let rewritten = match query {
        Query::Select {
            dataset,
            pattern,
            base_iri,
        } => Query::Select {
            dataset,
            pattern: cursor.pattern(pattern)?,
            base_iri,
        },
        Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => Query::Ask {
            dataset,
            pattern: cursor.pattern(pattern)?,
            base_iri,
        },
        Query::Construct {
            template,
            dataset,
            pattern,
            base_iri,
        } => Query::Construct {
            template,
            dataset,
            pattern: cursor.pattern(pattern)?,
            base_iri,
        },
        Query::Describe {
            dataset,
            pattern,
            base_iri,
        } => Query::Describe {
            dataset,
            pattern: cursor.pattern(pattern)?,
            base_iri,
        },
    };
    if cursor.ordinal != results.len() {
        return Err(ExactAlgebraError::ResultSet);
    }
    Ok(rewritten)
}

struct ResultCursor<'a> {
    results: &'a [DirectBgpResult],
    ordinal: usize,
    expanded_rows: usize,
    max_expanded_rows: usize,
}

impl ResultCursor<'_> {
    fn pattern(&mut self, pattern: GraphPattern) -> Result<GraphPattern, ExactAlgebraError> {
        Ok(match pattern {
            GraphPattern::Bgp { .. } => self.values()?,
            GraphPattern::Path {
                subject,
                path,
                object,
            } => GraphPattern::Path {
                subject,
                path,
                object,
            },
            GraphPattern::Join { left, right } => GraphPattern::Join {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
            },
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => GraphPattern::LeftJoin {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
                expression: expression.map(|value| self.expression(value)).transpose()?,
            },
            GraphPattern::Lateral { left, right } => GraphPattern::Lateral {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
            },
            GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
                expr: self.expression(expr)?,
                inner: Box::new(self.pattern(*inner)?),
            },
            GraphPattern::Union { left, right } => GraphPattern::Union {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
            },
            GraphPattern::Graph { name, inner } => GraphPattern::Graph {
                name,
                inner: Box::new(self.pattern(*inner)?),
            },
            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => GraphPattern::Extend {
                inner: Box::new(self.pattern(*inner)?),
                variable,
                expression: self.expression(expression)?,
            },
            GraphPattern::Minus { left, right } => GraphPattern::Minus {
                left: Box::new(self.pattern(*left)?),
                right: Box::new(self.pattern(*right)?),
            },
            GraphPattern::Values {
                variables,
                bindings,
            } => GraphPattern::Values {
                variables,
                bindings,
            },
            GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
                inner: Box::new(self.pattern(*inner)?),
                expression: expression
                    .into_iter()
                    .map(|value| self.order_expression(value))
                    .collect::<Result<_, _>>()?,
            },
            GraphPattern::Project { inner, variables } => GraphPattern::Project {
                inner: Box::new(self.pattern(*inner)?),
                variables,
            },
            GraphPattern::Distinct { inner } => GraphPattern::Distinct {
                inner: Box::new(self.pattern(*inner)?),
            },
            GraphPattern::Reduced { inner } => GraphPattern::Reduced {
                inner: Box::new(self.pattern(*inner)?),
            },
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => GraphPattern::Slice {
                inner: Box::new(self.pattern(*inner)?),
                start,
                length,
            },
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => GraphPattern::Group {
                inner: Box::new(self.pattern(*inner)?),
                variables,
                aggregates: aggregates
                    .into_iter()
                    .map(|(variable, aggregate)| {
                        Ok((variable, self.aggregate(aggregate)?))
                    })
                    .collect::<Result<_, ExactAlgebraError>>()?,
            },
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => GraphPattern::Service {
                name,
                // SERVICE is evaluated by its remote endpoint, not under the local OWL regime.
                inner,
                silent,
            },
        })
    }

    fn values(&mut self) -> Result<GraphPattern, ExactAlgebraError> {
        let result = self.results.get(self.ordinal).ok_or(ExactAlgebraError::ResultSet)?;
        validate_direct_bgp_result(result).map_err(|_| ExactAlgebraError::ResultSet)?;
        if result.outcome.status != DirectBgpStatus::Complete
            || result.outcome.exactness != DirectBgpExactness::Exact
            || result.outcome.completeness != DirectBgpCompleteness::Complete
        {
            return Err(ExactAlgebraError::ResultSet);
        }
        self.ordinal = self.ordinal.checked_add(1).ok_or(ExactAlgebraError::ResultSet)?;
        let variables = result
            .variables
            .iter()
            .map(|name| Variable::new(name).map_err(|_| ExactAlgebraError::Term))
            .collect::<Result<Vec<_>, _>>()?;
        let mut bindings = Vec::new();
        for solution in &result.solutions {
            let row = result
                .variables
                .iter()
                .map(|name| {
                    solution
                        .bindings
                        .get(name)
                        .map(ground_term)
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let multiplicity = usize::try_from(solution.multiplicity)
                .map_err(|_| ExactAlgebraError::RowCeiling)?;
            self.expanded_rows = self
                .expanded_rows
                .checked_add(multiplicity)
                .filter(|rows| *rows <= self.max_expanded_rows)
                .ok_or(ExactAlgebraError::RowCeiling)?;
            bindings.extend(std::iter::repeat_n(row, multiplicity));
        }
        Ok(GraphPattern::Values {
            variables,
            bindings,
        })
    }

    fn order_expression(
        &mut self,
        expression: OrderExpression,
    ) -> Result<OrderExpression, ExactAlgebraError> {
        Ok(match expression {
            OrderExpression::Asc(value) => OrderExpression::Asc(self.expression(value)?),
            OrderExpression::Desc(value) => OrderExpression::Desc(self.expression(value)?),
        })
    }

    fn aggregate(
        &mut self,
        aggregate: AggregateExpression,
    ) -> Result<AggregateExpression, ExactAlgebraError> {
        Ok(match aggregate {
            AggregateExpression::CountSolutions { distinct } => {
                AggregateExpression::CountSolutions { distinct }
            }
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => AggregateExpression::FunctionCall {
                name,
                expr: self.expression(expr)?,
                distinct,
            },
        })
    }

    fn expression(&mut self, expression: Expression) -> Result<Expression, ExactAlgebraError> {
        Ok(match expression {
            Expression::NamedNode(value) => Expression::NamedNode(value),
            Expression::Literal(value) => Expression::Literal(value),
            Expression::Variable(value) => Expression::Variable(value),
            Expression::Or(left, right) => Expression::Or(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::And(left, right) => Expression::And(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::Equal(left, right) => Expression::Equal(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::SameTerm(left, right) => Expression::SameTerm(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::Greater(left, right) => Expression::Greater(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::GreaterOrEqual(left, right) => Expression::GreaterOrEqual(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::Less(left, right) => Expression::Less(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::LessOrEqual(left, right) => Expression::LessOrEqual(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::In(left, values) => Expression::In(
                Box::new(self.expression(*left)?),
                values
                    .into_iter()
                    .map(|value| self.expression(value))
                    .collect::<Result<_, _>>()?,
            ),
            Expression::Add(left, right) => Expression::Add(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::Subtract(left, right) => Expression::Subtract(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::Multiply(left, right) => Expression::Multiply(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::Divide(left, right) => Expression::Divide(
                Box::new(self.expression(*left)?),
                Box::new(self.expression(*right)?),
            ),
            Expression::UnaryPlus(inner) => {
                Expression::UnaryPlus(Box::new(self.expression(*inner)?))
            }
            Expression::UnaryMinus(inner) => {
                Expression::UnaryMinus(Box::new(self.expression(*inner)?))
            }
            Expression::Not(inner) => Expression::Not(Box::new(self.expression(*inner)?)),
            Expression::Exists(pattern) => Expression::Exists(Box::new(self.pattern(*pattern)?)),
            Expression::Bound(variable) => Expression::Bound(variable),
            Expression::If(condition, yes, no) => Expression::If(
                Box::new(self.expression(*condition)?),
                Box::new(self.expression(*yes)?),
                Box::new(self.expression(*no)?),
            ),
            Expression::Coalesce(values) => Expression::Coalesce(
                values
                    .into_iter()
                    .map(|value| self.expression(value))
                    .collect::<Result<_, _>>()?,
            ),
            Expression::FunctionCall(function, arguments) => Expression::FunctionCall(
                function,
                arguments
                    .into_iter()
                    .map(|value| self.expression(value))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

fn ground_term(term: &DirectBgpRdfTerm) -> Result<GroundTerm, ExactAlgebraError> {
    match term {
        DirectBgpRdfTerm::Iri { value } => NamedNode::new(value.clone())
            .map(GroundTerm::NamedNode)
            .map_err(|_| ExactAlgebraError::Term),
        DirectBgpRdfTerm::BlankNode { .. } => {
            // The currently qualified exact engine rejects anonymous-individual mappings. Do not
            // silently convert a dataset blank node into a VALUES constant.
            Err(ExactAlgebraError::Term)
        }
        DirectBgpRdfTerm::Literal {
            lexical_form,
            datatype_iri,
            language,
        } => {
            let literal = if let Some(language) = language {
                Literal::new_language_tagged_literal(lexical_form.clone(), language.clone())
                    .map_err(|_| ExactAlgebraError::Term)?
            } else {
                let datatype =
                    NamedNode::new(datatype_iri.clone()).map_err(|_| ExactAlgebraError::Term)?;
                Literal::new_typed_literal(lexical_form.clone(), datatype)
            };
            Ok(GroundTerm::Literal(literal))
        }
    }
}
