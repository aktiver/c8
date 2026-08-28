package io.ngkg.reasoner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.NTriplesDocumentFormat;
import org.semanticweb.owlapi.io.StringDocumentSource;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.profiles.OWL2DLProfile;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;

/** Phase 40.8 exhaustive grounded OWL 2 Direct-Semantics evaluator. */
final class DirectBgpExecutor {
    private static final ObjectMapper JSON = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private static final String ENGINE = "hermit-grounded-owl2dl-isentailed-v1";
    private static final String REASONER_NAME = "HermiT";
    private static final String REASONER_VERSION = "1.4.5.519";
    private static final String ADAPTER_VERSION = "40.9";
    private static final int FORMAT_VERSION = 1;
    private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    private static final String RDFS_DATATYPE = "http://www.w3.org/2000/01/rdf-schema#Datatype";
    private static final String OWL = "http://www.w3.org/2002/07/owl#";

    private DirectBgpExecutor() {}

    static void run(Path requestPath) throws Exception {
        ExactRequest request = JSON.readValue(requestPath.toFile(), ExactRequest.class);
        validateRequest(request);
        verifyInputs(request.inputs(), request.aggregateInputSha256());
        Loaded loaded = loadMergedOntology(request.inputs());
        OWL2DLProfile profile = new OWL2DLProfile();
        if (!profile.checkOntology(loaded.merged()).isInProfile()) {
            throw new IllegalArgumentException("active exact ontology is not valid OWL 2 DL");
        }
        OWLReasoner reasoner = new Reasoner.ReasonerFactory().createReasoner(loaded.merged());
        try {
            if (!reasoner.isConsistent()) {
                throw new IllegalArgumentException("active exact ontology is inconsistent");
            }
            List<VariableDomain> domains = buildDomains(request.template(), loaded.merged());
            long candidateCount = checkedCandidateCount(domains, request.maxCandidateBindings());
            String candidateSpaceSha256 = candidateSpaceSha256(request, domains, candidateCount);
            String requestSha256 = hex(sha256(requestPath));
            long start = partitionBoundary(candidateCount, request.partition().index(), request.partition().count());
            long end = partitionBoundary(candidateCount, request.partition().index() + 1, request.partition().count());
            if (end - start > request.maxPartitionCandidates()) {
                throw new IllegalArgumentException("exact candidate partition exceeds maxPartitionCandidates");
            }
            List<EntailedBinding> entailed = new ArrayList<>();
            long groundedOwl2Dl = 0;
            long reasonerRequests = 0;
            for (long ordinal = start; ordinal < end; ordinal++) {
                Map<String, RdfTerm> binding = unrank(ordinal, domains);
                Grounded grounded = ground(request.template(), binding, loaded.merged(), ordinal, request.maxGroundedRdfBytesPerCandidate());
                if (grounded.targetAxioms().size() > request.maxGroundedAxiomsPerCandidate()) {
                    throw new IllegalArgumentException("grounded candidate exceeds maxGroundedAxiomsPerCandidate");
                }
                OWLOntologyManager checker = OWLManager.createOWLOntologyManager();
                Set<OWLAxiom> combined = new HashSet<>(loaded.merged().getAxioms(Imports.INCLUDED));
                combined.addAll(grounded.targetAxioms());
                OWLOntology candidateCombined = checker.createOntology(combined);
                if (!profile.checkOntology(candidateCombined).isInProfile()) {
                    checker.removeOntology(candidateCombined);
                    continue; // W3C C3: instantiated BGP must keep O(SG)+axioms in OWL 2 DL.
                }
                checker.removeOntology(candidateCombined);
                groundedOwl2Dl++;
                reasonerRequests++;
                // W3C C1 requires only the logical axioms of the instantiated BGP to be entailed.
                // Declarations and annotations are non-logical and are constrained by C2/C3 instead.
                if (reasoner.isEntailed(grounded.logicalAxioms())) {
                    entailed.add(new EntailedBinding(ordinal, new TreeMap<>(binding), grounded.groundedRdfSha256(), grounded.logicalAxiomsSha256(), grounded.logicalAxioms().size()));
                }
            }
            PartitionResult result = new PartitionResult(
                    FORMAT_VERSION, request.datasetId(), request.snapshotId(), request.querySha256(),
                    request.bgpSha256(), ENGINE, REASONER_NAME, REASONER_VERSION, ADAPTER_VERSION,
                    requestSha256, request.aggregateInputSha256(), candidateSpaceSha256, request.partition(), candidateCount, start, end,
                    end - start, groundedOwl2Dl, entailed.size(), reasonerRequests,
                    List.copyOf(entailed), true
            );
            Path output = Path.of(request.outputPath());
            if (output.getParent() != null) Files.createDirectories(output.getParent());
            Path temporary = output.resolveSibling(output.getFileName() + ".tmp-" + UUID.randomUUID());
            JSON.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), result);
            try {
                Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            reasoner.dispose();
        }
    }

    private static Loaded loadMergedOntology(List<InputArtifact> inputs) throws Exception {
        OWLOntologyManager loader = OWLManager.createOWLOntologyManager();
        Map<String, IRI> mappings = new HashMap<>();
        for (InputArtifact input : inputs) {
            IRI document = IRI.create(Path.of(input.path()).toUri());
            for (String alias : input.ontologyIris()) {
                IRI previous = mappings.putIfAbsent(alias, document);
                if (previous != null && !previous.equals(document)) {
                    throw new IllegalArgumentException("ontology IRI maps to multiple exact input documents");
                }
            }
        }
        loader.getIRIMappers().add(ontologyIri -> {
            IRI mapped = mappings.get(ontologyIri.toString());
            if (mapped == null) throw new IllegalArgumentException("unmapped exact ontology import: " + ontologyIri);
            return mapped;
        });
        for (InputArtifact input : inputs) {
            loader.loadOntologyFromOntologyDocument(Path.of(input.path()).toFile());
        }
        Set<OWLAxiom> axioms = new HashSet<>();
        loader.getOntologies().forEach(ontology -> axioms.addAll(ontology.getAxioms(Imports.INCLUDED)));
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        return new Loaded(manager.createOntology(axioms));
    }

    private static List<VariableDomain> buildDomains(BgpTemplate template, OWLOntology ontology) {
        Map<String, VariableSpec> declared = template.variables().stream()
                .collect(Collectors.toMap(VariableSpec::name, value -> value, (a,b) -> { throw new IllegalArgumentException("duplicate exact variable"); }, TreeMap::new));
        List<VariableDomain> domains = new ArrayList<>();
        for (VariableSpec variable : declared.values()) {
            TreeMap<String, RdfTerm> values = new TreeMap<>();
            switch (variable.role()) {
                case "class" -> ontology.getClassesInSignature(Imports.INCLUDED).forEach(entity -> putIri(values, entity.getIRI().toString()));
                case "object-property" -> ontology.getObjectPropertiesInSignature(Imports.INCLUDED).forEach(entity -> putIri(values, entity.getIRI().toString()));
                case "data-property" -> ontology.getDataPropertiesInSignature(Imports.INCLUDED).forEach(entity -> putIri(values, entity.getIRI().toString()));
                case "annotation-property" -> ontology.getAnnotationPropertiesInSignature().forEach(entity -> putIri(values, entity.getIRI().toString()));
                case "datatype" -> ontology.getDatatypesInSignature(Imports.INCLUDED).forEach(entity -> putIri(values, entity.getIRI().toString()));
                case "named-individual" -> {
                    ontology.getIndividualsInSignature(Imports.INCLUDED).forEach(entity -> putIri(values, entity.getIRI().toString()));
                    if ("structural-position".equals(variable.source()) && !ontology.getAnonymousIndividuals().isEmpty()) {
                        // A variable in the OWL Individual grammar may map to anonymous individuals.
                        // Phase 40.8 does not claim σ/anonymous-instance-map multiplicity yet; fail
                        // closed rather than certify a named-only subset as complete.
                        throw new IllegalArgumentException("anonymous individual candidate mappings require later W3C qualification");
                    }
                }
                case "literal" -> ontology.getAxioms(Imports.INCLUDED).forEach(axiom -> axiom.components()
                        .filter(component -> component instanceof OWLLiteral)
                        .map(component -> (OWLLiteral) component)
                        .forEach(literal -> putLiteral(values, literal)));
                default -> throw new IllegalArgumentException("unsupported exact variable role: " + variable.role());
            }
            // Built-ins are valid finite candidate terms even when absent from the user signature.
            if ("class".equals(variable.role())) { putIri(values, OWL + "Thing"); putIri(values, OWL + "Nothing"); }
            if ("object-property".equals(variable.role())) { putIri(values, OWL + "topObjectProperty"); putIri(values, OWL + "bottomObjectProperty"); }
            if ("data-property".equals(variable.role())) { putIri(values, OWL + "topDataProperty"); putIri(values, OWL + "bottomDataProperty"); }
            if ("annotation-property".equals(variable.role())) {
                putIri(values, "http://www.w3.org/2000/01/rdf-schema#label");
                putIri(values, "http://www.w3.org/2000/01/rdf-schema#comment");
                putIri(values, "http://www.w3.org/2000/01/rdf-schema#seeAlso");
                putIri(values, "http://www.w3.org/2000/01/rdf-schema#isDefinedBy");
                putIri(values, OWL + "versionInfo");
                putIri(values, OWL + "deprecated");
            }
            if (values.isEmpty()) {
                // Empty domain means the exact candidate product is empty, which is a complete empty answer.
                domains.add(new VariableDomain(variable.name(), List.of()));
            } else {
                domains.add(new VariableDomain(variable.name(), List.copyOf(values.values())));
            }
        }
        return domains;
    }



    private static String candidateSpaceSha256(ExactRequest request, List<VariableDomain> domains, long candidateCount) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update("ngkg-direct-exact-candidate-space-v1\0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update(request.bgpSha256().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update(request.aggregateInputSha256().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(candidateCount).array());
        for (VariableDomain domain : domains) {
            byte[] name = domain.name().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(name.length).array());
            digest.update(name);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(domain.values().size()).array());
            for (RdfTerm term : domain.values()) {
                byte[] value = ntriples(term).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value.length).array());
                digest.update(value);
            }
        }
        return hex(digest.digest());
    }
    private static long checkedCandidateCount(List<VariableDomain> domains, long ceiling) {
        BigInteger count = BigInteger.ONE;
        for (VariableDomain domain : domains) count = count.multiply(BigInteger.valueOf(domain.values().size()));
        if (domains.isEmpty()) count = BigInteger.ONE; // ground BGP: exactly one entailment check.
        if (count.compareTo(BigInteger.valueOf(ceiling)) > 0 || count.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw new IllegalArgumentException("finite candidate binding space exceeds maxCandidateBindings");
        }
        return count.longValueExact();
    }

    private static long partitionBoundary(long total, int index, int count) {
        return BigInteger.valueOf(total).multiply(BigInteger.valueOf(index))
                .divide(BigInteger.valueOf(count)).longValueExact();
    }

    private static Map<String, RdfTerm> unrank(long ordinal, List<VariableDomain> domains) {
        TreeMap<String, RdfTerm> binding = new TreeMap<>();
        long remaining = ordinal;
        for (int i = domains.size() - 1; i >= 0; i--) {
            VariableDomain domain = domains.get(i);
            int radix = domain.values().size();
            if (radix == 0) return binding;
            int digit = (int) (remaining % radix);
            remaining /= radix;
            binding.put(domain.name(), domain.values().get(digit));
        }
        return binding;
    }

    private static Grounded ground(BgpTemplate template, Map<String, RdfTerm> binding, OWLOntology base, long candidateOrdinal, long maxRdfBytes) throws Exception {
        StringBuilder rdf = new StringBuilder();
        Set<String> explicitDeclarations = new HashSet<>();
        for (TriplePattern triple : template.triples()) {
            RdfTerm s = resolve(triple.subject(), binding);
            RdfTerm p = resolve(triple.predicate(), binding);
            RdfTerm o = resolve(triple.object(), binding);
            rdf.append(ntriples(s)).append(' ').append(ntriples(p)).append(' ').append(ntriples(o)).append(" .\n");
            if (isExplicitDeclaration(p, o) && "iri".equals(s.termType())) {
                explicitDeclarations.add(declarationKey(s.value(), o.value()));
            }
            injectDeclaration(rdf, s, base);
            injectDeclaration(rdf, p, base);
            injectDeclaration(rdf, o, base);
        }
        byte[] rdfBytes = rdf.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (rdfBytes.length > maxRdfBytes) throw new IllegalArgumentException("grounded candidate exceeds maxGroundedRdfBytesPerCandidate");
        String groundedRdfSha256 = hex(MessageDigest.getInstance("SHA-256").digest(rdfBytes));
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        StringDocumentSource source = new StringDocumentSource(
                rdf.toString(), IRI.create("urn:ngkg:direct-candidate:" + candidateOrdinal),
                new NTriplesDocumentFormat(), null
        );
        OWLOntology candidate = manager.loadOntologyFromOntologyDocument(source);
        Set<OWLAxiom> target = new HashSet<>();
        for (OWLAxiom axiom : candidate.getAxioms()) {
            if (axiom instanceof OWLDeclarationAxiom declaration) {
                String key = declarationKey(declaration.getEntity().getIRI().toString(), declarationTypeIri(declaration.getEntity()));
                if (!explicitDeclarations.contains(key)) continue;
            }
            target.add(axiom);
        }
        Set<OWLAxiom> logical = target.stream().filter(OWLAxiom::isLogicalAxiom).collect(Collectors.toCollection(HashSet::new));
        String logicalAxiomsSha256 = canonicalLogicalAxiomsSha256(logical);
        manager.removeOntology(candidate);
        return new Grounded(target, logical, groundedRdfSha256, logicalAxiomsSha256);
    }


    private static String canonicalLogicalAxiomsSha256(Set<OWLAxiom> axioms) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update("ngkg-direct-logical-axioms-v1\0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        List<String> canonical = axioms.stream().map(OWLAxiom::toString).sorted().toList();
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(canonical.size()).array());
        for (String axiom : canonical) {
            byte[] bytes = axiom.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(bytes.length).array());
            digest.update(bytes);
        }
        return hex(digest.digest());
    }

    private static void injectDeclaration(StringBuilder rdf, RdfTerm term, OWLOntology base) {
        if (!"iri".equals(term.termType())) return;
        IRI iri = IRI.create(term.value());
        if (base.containsClassInSignature(iri, Imports.INCLUDED)) declarationTriple(rdf, term.value(), OWL + "Class");
        if (base.containsObjectPropertyInSignature(iri, Imports.INCLUDED)) declarationTriple(rdf, term.value(), OWL + "ObjectProperty");
        if (base.containsDataPropertyInSignature(iri, Imports.INCLUDED)) declarationTriple(rdf, term.value(), OWL + "DatatypeProperty");
        if (base.containsAnnotationPropertyInSignature(iri)) declarationTriple(rdf, term.value(), OWL + "AnnotationProperty");
        if (base.containsIndividualInSignature(iri, Imports.INCLUDED)) declarationTriple(rdf, term.value(), OWL + "NamedIndividual");
        if (base.containsDatatypeInSignature(iri, Imports.INCLUDED)) declarationTriple(rdf, term.value(), RDFS_DATATYPE);
    }

    private static void declarationTriple(StringBuilder rdf, String iri, String type) {
        rdf.append('<').append(iri).append("> <").append(RDF_TYPE).append("> <").append(type).append("> .\n");
    }

    private static boolean isExplicitDeclaration(RdfTerm predicate, RdfTerm object) {
        if (!"iri".equals(predicate.termType()) || !RDF_TYPE.equals(predicate.value()) || !"iri".equals(object.termType())) return false;
        return Set.of(OWL + "Class", OWL + "ObjectProperty", OWL + "DatatypeProperty", OWL + "AnnotationProperty", OWL + "NamedIndividual", RDFS_DATATYPE).contains(object.value());
    }

    private static String declarationTypeIri(OWLEntity entity) {
        if (entity.isOWLClass()) return OWL + "Class";
        if (entity.isOWLObjectProperty()) return OWL + "ObjectProperty";
        if (entity.isOWLDataProperty()) return OWL + "DatatypeProperty";
        if (entity.isOWLAnnotationProperty()) return OWL + "AnnotationProperty";
        if (entity.isOWLNamedIndividual()) return OWL + "NamedIndividual";
        if (entity.isOWLDatatype()) return RDFS_DATATYPE;
        throw new IllegalArgumentException("unsupported declaration entity");
    }

    private static String declarationKey(String iri, String type) { return iri + "\u0000" + type; }
    private static RdfTerm resolve(TermPattern pattern, Map<String, RdfTerm> binding) {
        if ("variable".equals(pattern.termType())) {
            RdfTerm value = binding.get(pattern.name());
            if (value == null) throw new IllegalArgumentException("candidate binding is missing variable " + pattern.name());
            return value;
        }
        return new RdfTerm(pattern.termType(), pattern.value(), pattern.lexicalForm(), pattern.datatypeIri(), pattern.language());
    }

    private static String ntriples(RdfTerm term) {
        return switch (term.termType()) {
            case "iri" -> "<" + term.value() + ">";
            case "blankNode" -> "_:" + safeBlank(term.value());
            case "literal" -> literalNTriples(term);
            default -> throw new IllegalArgumentException("unresolved variable in grounded candidate");
        };
    }
    private static String literalNTriples(RdfTerm term) {
        String lexical = term.lexicalForm().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
        if (term.language() != null) return "\"" + lexical + "\"@" + term.language();
        return "\"" + lexical + "\"^^<" + term.datatypeIri() + ">";
    }
    private static String safeBlank(String value) { return value.replaceAll("[^A-Za-z0-9_.-]", "_"); }

    private static void putIri(Map<String,RdfTerm> values, String iri) { values.put("I\u0000" + iri, new RdfTerm("iri", iri, null, null, null)); }
    private static void putLiteral(Map<String,RdfTerm> values, OWLLiteral literal) {
        String datatype = literal.getDatatype().getIRI().toString();
        String language = literal.hasLang() ? literal.getLang() : null;
        RdfTerm term = new RdfTerm("literal", null, literal.getLiteral(), datatype, language);
        values.put("L\u0000" + literal.toString(), term);
    }

    private static void validateRequest(ExactRequest request) {
        if (request.formatVersion() != FORMAT_VERSION || !ENGINE.equals(request.engine())) throw new IllegalArgumentException("unsupported exact request");
        if (request.partition().count() <= 0 || request.partition().index() < 0 || request.partition().index() >= request.partition().count()) throw new IllegalArgumentException("invalid exact partition");
        if (request.maxCandidateBindings() <= 0 || request.maxPartitionCandidates() <= 0
                || request.maxGroundedAxiomsPerCandidate() <= 0 || request.maxGroundedRdfBytesPerCandidate() <= 0)
            throw new IllegalArgumentException("invalid exact ceilings");
        if (!request.bgpSha256().equals(request.template().bgpSha256())) throw new IllegalArgumentException("BGP hash/template mismatch");
    }

    private static void verifyInputs(List<InputArtifact> inputs, String expectedAggregate) throws Exception {
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        for (InputArtifact input : inputs) {
            byte[] observed = sha256(Path.of(input.path()));
            if (!hex(observed).equals(input.sha256())) throw new IllegalArgumentException("exact input SHA mismatch");
            aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(observed.length).array());
            aggregate.update(observed);
        }
        if (!hex(aggregate.digest()).equals(expectedAggregate)) throw new IllegalArgumentException("exact aggregate input SHA mismatch");
    }
    private static byte[] sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[1024 * 1024];
            for (int read; (read = input.read(buffer)) >= 0;) { if (read > 0) digest.update(buffer, 0, read); }
        }
        return digest.digest();
    }
    private static String hex(byte[] bytes) { return HexFormat.of().formatHex(bytes); }

    record Loaded(OWLOntology merged) {}
    record InputArtifact(String path, String sha256, List<String> ontologyIris) {}
    record Partition(int index, int count) {}
    record VariableSpec(String name, String role, String source) {}
    @JsonIgnoreProperties(ignoreUnknown = false)
    record TermPattern(String termType, String name, String value, String lexicalForm, String datatypeIri, String language) {}
    record TriplePattern(TermPattern subject, TermPattern predicate, TermPattern object) {}
    record GraphScope(String scope, String graphIri, String variable) {}
    record BgpTemplate(long ordinal, String bgpSha256, GraphScope graphScope, List<VariableSpec> variables, List<TriplePattern> triples) {}
    record ExactRequest(int formatVersion, UUID datasetId, UUID snapshotId, String querySha256, String sparqlAlgebraSha256,
                        String bgpSha256, String activeDatasetSha256, String authorizedGraphSetSha256,
                        String owlSignatureSha256, String datatypePolicySha256, String owlProfileQualificationSha256,
                        String owlConsistencyQualificationSha256, String engine, List<InputArtifact> inputs,
                        String aggregateInputSha256, BgpTemplate template, Partition partition, long maxCandidateBindings,
                        long maxPartitionCandidates, long maxGroundedAxiomsPerCandidate,
                        long maxGroundedRdfBytesPerCandidate, String outputPath) {}
    record RdfTerm(String termType, String value, String lexicalForm, String datatypeIri, String language) {}
    record VariableDomain(String name, List<RdfTerm> values) {}
    record Grounded(Set<OWLAxiom> targetAxioms, Set<OWLAxiom> logicalAxioms, String groundedRdfSha256, String logicalAxiomsSha256) {}
    record EntailedBinding(long candidateOrdinal, Map<String,RdfTerm> bindings, String groundedRdfSha256, String logicalAxiomsSha256, long logicalAxiomCount) {}
    record PartitionResult(int formatVersion, UUID datasetId, UUID snapshotId, String querySha256, String bgpSha256,
                           String engine, String reasonerName, String reasonerVersion, String adapterVersion,
                           String requestSha256, String aggregateInputSha256, String candidateSpaceSha256,
                           Partition partition, long candidateBindingCount, long partitionStartOrdinal,
                           long partitionEndOrdinalExclusive, long checkedCandidateCount, long groundedOwl2dlCandidateCount,
                           long entailedCandidateCount, long reasonerRequestCount, List<EntailedBinding> entailed, boolean complete) {}
}
