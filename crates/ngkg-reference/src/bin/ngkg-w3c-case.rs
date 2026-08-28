//! One-process W3C conformance case driver used by the manifest harness.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use ngkg_reference::{
    CertifiedQueryExecutionLimits, CompiledSparqlQuery, DefaultDatasetPolicy, ExecutedQueryResult,
    execute_compiled_query_with_default_policy, load_rdf_fixture_with_base_iri, parse_expected,
    verify_expected,
};
use oxigraph::model::dataset::CanonicalizationAlgorithm;
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{BlankNode, Dataset, Literal, NamedNode, Term, Variable},
    sparql::results::{QueryResultsFormat, QueryResultsSerializer},
    store::Store,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NamedData {
    path: PathBuf,
    graph_iri: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Case {
    kind: String,
    action: Option<PathBuf>,
    base_iri: Option<String>,
    query: Option<PathBuf>,
    default_data: Vec<PathBuf>,
    named_data: Vec<NamedData>,
    expected: Option<PathBuf>,
    expected_parse_success: Option<bool>,
}

fn rdf_format(path: &Path) -> Result<RdfFormat, String> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    RdfFormat::from_extension(extension)
        .ok_or_else(|| format!("unsupported RDF fixture extension: {}", path.display()))
}

fn file_base_iri(path: &Path) -> Result<String, String> {
    url::Url::from_file_path(path)
        .map(String::from)
        .map_err(|()| format!("cannot construct file IRI for {}", path.display()))
}

fn parse_dataset(
    path: &Path,
    format: RdfFormat,
    base_iri: Option<&str>,
) -> Result<Dataset, String> {
    let input = fs::File::open(path).map_err(|error| error.to_string())?;
    RdfParser::from_format(format)
        .with_base_iri(match base_iri {
            Some(value) => value.to_owned(),
            None => file_base_iri(path)?,
        })
        .map_err(|error| error.to_string())?
        .for_reader(input)
        .collect::<Result<Dataset, _>>()
        .map_err(|error| error.to_string())
}

fn syntax_case(case: &Case, trig: bool) -> Result<(), String> {
    let path = case
        .action
        .as_ref()
        .ok_or_else(|| "syntax case has no action".to_owned())?;
    let expected = case
        .expected_parse_success
        .ok_or_else(|| "syntax case lacks expectedParseSuccess".to_owned())?;
    let parse_result = if trig {
        parse_dataset(path, RdfFormat::TriG, case.base_iri.as_deref()).map(|_| ())
    } else {
        fs::read_to_string(path)
            .map_err(|error| error.to_string())
            .and_then(|query| {
                match case.base_iri.as_deref() {
                    Some(base_iri) => CompiledSparqlQuery::parse_with_base_iri(&query, base_iri),
                    None => CompiledSparqlQuery::parse(&query),
                }
                .map(|_| ())
                .map_err(|error| error.to_string())
            })
    };
    let observed = parse_result.is_ok();
    if observed == expected {
        Ok(())
    } else {
        Err(format!(
            "syntax outcome mismatch: expected parse={expected}, observed parse={observed}; parser={}",
            parse_result
                .err()
                .unwrap_or_else(|| "accepted input that should be rejected".to_owned())
        ))
    }
}

fn trig_evaluation_case(case: &Case) -> Result<(), String> {
    let action = case
        .action
        .as_ref()
        .ok_or_else(|| "TriG evaluation case has no action".to_owned())?;
    let expected = case
        .expected
        .as_ref()
        .ok_or_else(|| "TriG evaluation case has no expected dataset".to_owned())?;
    let mut observed_dataset = parse_dataset(action, RdfFormat::TriG, case.base_iri.as_deref())?;
    let mut expected_dataset = parse_dataset(expected, rdf_format(expected)?, None)?;
    observed_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    expected_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    if observed_dataset == expected_dataset {
        Ok(())
    } else {
        let observed_only = (&observed_dataset)
            .into_iter()
            .find(|quad| !expected_dataset.contains(*quad))
            .map(|quad| quad.to_string())
            .unwrap_or_else(|| "<none>".to_owned());
        let expected_only = (&expected_dataset)
            .into_iter()
            .find(|quad| !observed_dataset.contains(*quad))
            .map(|quad| quad.to_string())
            .unwrap_or_else(|| "<none>".to_owned());
        Err(format!(
            "TriG dataset differs from normative W3C result {} (observed {} quads, expected {}; observed-only: {}; expected-only: {})",
            expected.display(),
            observed_dataset.len(),
            expected_dataset.len(),
            observed_only,
            expected_only
        ))
    }
}

fn json_term(value: &serde_json::Value) -> Result<Term, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "SPARQL result term is not an object".to_owned())?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "SPARQL result term has no type".to_owned())?;
    let lexical = object
        .get("value")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "SPARQL result term has no value".to_owned())?;
    match kind {
        "uri" => NamedNode::new(lexical.to_owned())
            .map(Term::NamedNode)
            .map_err(|error| error.to_string()),
        "bnode" => BlankNode::new(lexical.to_owned())
            .map(Term::BlankNode)
            .map_err(|error| error.to_string()),
        "literal" | "typed-literal" => {
            if let Some(language) = object.get("xml:lang").and_then(serde_json::Value::as_str) {
                Literal::new_language_tagged_literal(lexical.to_owned(), language.to_owned())
                    .map(Term::Literal)
                    .map_err(|error| error.to_string())
            } else if let Some(datatype) =
                object.get("datatype").and_then(serde_json::Value::as_str)
            {
                let datatype =
                    NamedNode::new(datatype.to_owned()).map_err(|error| error.to_string())?;
                Ok(Term::Literal(Literal::new_typed_literal(
                    lexical.to_owned(),
                    datatype,
                )))
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(
                    lexical.to_owned(),
                )))
            }
        }
        other => Err(format!("unsupported SPARQL result term type {other}")),
    }
}

fn execute_case(case: &Case) -> Result<(CompiledSparqlQuery, ExecutedQueryResult), String> {
    let query_path = case
        .query
        .as_ref()
        .ok_or_else(|| "query evaluation has no query path".to_owned())?;
    let query = fs::read_to_string(query_path).map_err(|error| error.to_string())?;
    let compiled = match case.base_iri.as_deref() {
        Some(base_iri) => CompiledSparqlQuery::parse_with_base_iri(&query, base_iri),
        None => CompiledSparqlQuery::parse(&query),
    }
    .map_err(|error| error.to_string())?;
    let store = Store::new().map_err(|error| error.to_string())?;
    for path in &case.default_data {
        load_rdf_fixture_with_base_iri(
            &store,
            path,
            rdf_format(path)?,
            None,
            &file_base_iri(path)?,
        )
        .map_err(|error| error.to_string())?;
    }
    for item in &case.named_data {
        let graph = NamedNode::new(item.graph_iri.clone()).map_err(|error| error.to_string())?;
        load_rdf_fixture_with_base_iri(
            &store,
            &item.path,
            rdf_format(&item.path)?,
            Some(graph),
            &file_base_iri(&item.path)?,
        )
        .map_err(|error| error.to_string())?;
    }
    let limits = CertifiedQueryExecutionLimits {
        max_solution_rows: 2_000_000,
        max_graph_triples: 2_000_000,
        max_graph_blank_nodes: 200_000,
    };
    let observed = execute_compiled_query_with_default_policy(
        &store,
        &compiled,
        limits,
        DefaultDatasetPolicy::StoredDefault,
    )
    .map_err(|error| error.to_string())?;
    Ok((compiled, observed))
}

fn parse_csv_records(bytes: &[u8]) -> Result<Vec<Vec<String>>, String> {
    let input = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut after_quote = false;
    let mut characters = input.chars().peekable();
    while let Some(character) = characters.next() {
        if quoted {
            if character == '"' {
                if characters.peek() == Some(&'"') {
                    field.push('"');
                    let _ = characters.next();
                } else {
                    quoted = false;
                    after_quote = true;
                }
            } else {
                field.push(character);
            }
            continue;
        }
        if after_quote && !matches!(character, ',' | '\r' | '\n') {
            return Err("invalid character after closing CSV quote".to_owned());
        }
        match character {
            '"' if field.is_empty() && !after_quote => quoted = true,
            '"' => return Err("unexpected quote in unquoted CSV field".to_owned()),
            ',' => {
                record.push(std::mem::take(&mut field));
                after_quote = false;
            }
            '\r' => {
                if characters.peek() == Some(&'\n') {
                    let _ = characters.next();
                }
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
                after_quote = false;
            }
            '\n' => {
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
                after_quote = false;
            }
            _ => field.push(character),
        }
    }
    if quoted {
        return Err("unterminated quoted CSV field".to_owned());
    }
    if !record.is_empty() || !field.is_empty() || after_quote {
        record.push(field);
        records.push(record);
    }
    if let Some(width) = records.first().map(Vec::len)
        && let Some((index, record)) = records
            .iter()
            .enumerate()
            .find(|(_, record)| record.len() != width)
    {
        return Err(format!(
            "CSV record {index} has {} fields; expected {width}",
            record.len()
        ));
    }
    Ok(records)
}

fn csv_records_equivalent(observed: &[u8], expected: &[u8]) -> Result<bool, String> {
    let observed = parse_csv_records(observed)?;
    let expected = parse_csv_records(expected)?;
    if observed.len() != expected.len()
        || observed.first() != expected.first()
        || observed
            .iter()
            .map(Vec::len)
            .ne(expected.iter().map(Vec::len))
    {
        return Ok(false);
    }
    let mut observed_to_expected = BTreeMap::new();
    let mut expected_to_observed = BTreeMap::new();
    for (observed_record, expected_record) in observed.iter().skip(1).zip(expected.iter().skip(1)) {
        for (observed_field, expected_field) in observed_record.iter().zip(expected_record) {
            let observed_blank = observed_field.strip_prefix("_:");
            let expected_blank = expected_field.strip_prefix("_:");
            match (observed_blank, expected_blank) {
                (Some(observed_blank), Some(expected_blank)) => {
                    if observed_to_expected
                        .get(observed_blank)
                        .is_some_and(|mapped| *mapped != expected_blank)
                        || expected_to_observed
                            .get(expected_blank)
                            .is_some_and(|mapped| *mapped != observed_blank)
                    {
                        return Ok(false);
                    }
                    observed_to_expected.insert(observed_blank, expected_blank);
                    expected_to_observed.insert(expected_blank, observed_blank);
                }
                (None, None) if observed_field == expected_field => {}
                _ => return Ok(false),
            }
        }
    }
    Ok(true)
}

fn csv_result_format_case(case: &Case) -> Result<(), String> {
    let expected_path = case
        .expected
        .as_ref()
        .ok_or_else(|| "CSV result-format case has no expected result".to_owned())?;
    let (_, observed) = execute_case(case)?;
    let ExecutedQueryResult::Solutions(solutions) = observed else {
        return Err("CSV result-format case did not produce SELECT solutions".to_owned());
    };
    let variables = solutions
        .head
        .iter()
        .map(|name| Variable::new(name.clone()).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let allowed = solutions
        .head
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut output = Vec::new();
    {
        let mut writer = QueryResultsSerializer::from_format(QueryResultsFormat::Csv)
            .serialize_solutions_to_writer(&mut output, variables)
            .map_err(|error| error.to_string())?;
        for binding in &solutions.bindings {
            let object = binding
                .as_object()
                .ok_or_else(|| "SELECT solution is not a binding object".to_owned())?;
            let mut row = Vec::with_capacity(object.len());
            for (name, value) in object {
                if !allowed.contains(name.as_str()) {
                    return Err(format!("SELECT solution binds undeclared variable {name}"));
                }
                let variable = Variable::new(name.clone()).map_err(|error| error.to_string())?;
                row.push((variable, json_term(value)?));
            }
            writer
                .serialize(
                    row.iter()
                        .map(|(variable, term)| (variable.as_ref(), term.as_ref())),
                )
                .map_err(|error| error.to_string())?;
        }
        writer.finish().map_err(|error| error.to_string())?;
    }
    let expected = fs::read(expected_path).map_err(|error| error.to_string())?;
    if csv_records_equivalent(&output, &expected)? {
        Ok(())
    } else {
        Err(format!(
            "CSV serialization differs from normative W3C result {}; observed prefix={:?}; expected prefix={:?}",
            expected_path.display(),
            String::from_utf8_lossy(&output[..output.len().min(512)]),
            String::from_utf8_lossy(&expected[..expected.len().min(512)])
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{csv_records_equivalent, parse_csv_records};

    #[test]
    fn csv_parser_handles_quotes_commas_and_line_endings() {
        assert_eq!(
            parse_csv_records(b"a,b\r\n1,\"two,too\"\r\n").unwrap_or_default(),
            vec![vec!["a", "b"], vec!["1", "two,too"]]
        );
    }

    #[test]
    fn csv_equivalence_renames_blank_nodes_bijectively() {
        assert!(
            csv_records_equivalent(b"x,y\r\n_:left,_:left\r\n", b"x,y\n_:right,_:right\n")
                .unwrap_or(false)
        );
        assert!(
            !csv_records_equivalent(b"x,y\n_:left,_:left\n", b"x,y\n_:a,_:b\n").unwrap_or(true)
        );
    }

    #[test]
    fn csv_equivalence_does_not_hide_lexical_changes() {
        assert!(!csv_records_equivalent(b"x\n1000000\n", b"x\n1.0E6\n").unwrap_or(true));
    }
}

fn query_case(case: &Case) -> Result<(), String> {
    let expected_path = case
        .expected
        .as_ref()
        .ok_or_else(|| "query evaluation has no expected result".to_owned())?;
    let (compiled, observed) = execute_case(case)?;
    let limits = CertifiedQueryExecutionLimits {
        max_solution_rows: 2_000_000,
        max_graph_triples: 2_000_000,
        max_graph_blank_nodes: 200_000,
    };
    let bytes = fs::read(expected_path).map_err(|error| error.to_string())?;
    let expected = parse_expected(expected_path, &bytes, compiled.form(), limits)
        .map_err(|error| error.to_string())?;
    verify_expected(
        &observed,
        &expected,
        compiled.solution_order_is_significant(),
        limits,
    )
    .map(|_| ())
    .map_err(|error| error.to_string())
}

fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();
    let Some(case_path) = args.next() else {
        eprintln!("usage: ngkg-w3c-case CASE.json");
        std::process::exit(2);
    };
    if args.next().is_some() {
        eprintln!("ngkg-w3c-case accepts exactly one case file");
        std::process::exit(2);
    }
    let result = fs::read(&case_path)
        .map_err(|error| error.to_string())
        .and_then(|bytes| serde_json::from_slice::<Case>(&bytes).map_err(|error| error.to_string()))
        .and_then(|case| match case.kind.as_str() {
            "trig-syntax" => syntax_case(&case, true),
            "trig-evaluation" => trig_evaluation_case(&case),
            "sparql-syntax" => syntax_case(&case, false),
            "query-evaluation" => query_case(&case),
            "csv-result-format" => csv_result_format_case(&case),
            other => Err(format!("unsupported W3C case kind: {other}")),
        });
    match result {
        Ok(()) => println!("{}", json!({"status":"pass"})),
        Err(error) => {
            println!("{}", json!({"status":"fail","message":error}));
            std::process::exit(1);
        }
    }
}
