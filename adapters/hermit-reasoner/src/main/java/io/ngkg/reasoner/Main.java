package io.ngkg.reasoner;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.NTriplesDocumentFormat;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.profiles.OWL2DLProfile;
import org.semanticweb.owlapi.profiles.OWLProfileReport;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Version-locked command adapter around HermiT and OWLAPI.
 *
 * <p>The adapter checks every input checksum, checks ontology consistency, and materializes
 * finite consequences over named entities. It deliberately reports that it does not emit a
 * proof DAG and that the output is not a finite representation of every OWL 2 DL consequence.</p>
 */
public final class Main {
    private static final String REASONER_NAME = "HermiT";
    private static final String REASONER_VERSION = "1.4.5.519";
    private static final String OWL_PROFILE = "OWL 2 DL";
    private static final int MAX_PROFILE_VIOLATION_SAMPLES = 100;
    private static final int MAX_PROFILE_VIOLATION_CHARACTERS = 4096;
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Main() {
    }

    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                throw new IllegalArgumentException("usage: java -jar ngkg-hermit-adapter.jar (--request|--direct-request) PATH");
            }
            if ("--request".equals(args[0])) {
                run(Path.of(args[1]));
            } else if ("--direct-request".equals(args[0])) {
                DirectBgpExecutor.run(Path.of(args[1]));
            } else {
                throw new IllegalArgumentException("usage: java -jar ngkg-hermit-adapter.jar (--request|--direct-request) PATH");
            }
        } catch (Exception error) {
            error.printStackTrace(System.err);
            System.exit(1);
        }
    }

    static void run(Path requestPath) throws Exception {
        Request request = JSON.readValue(requestPath.toFile(), Request.class);
        if (request.formatVersion() != 4 || request.inputs() == null || request.inputs().isEmpty()) {
            throw new IllegalArgumentException("unsupported or empty reasoner request");
        }
        verifyInputs(request);

        Set<OWLAxiom> mergedAxioms = new HashSet<>();
        OWLOntologyManager loader = OWLManager.createOWLOntologyManager();
        Map<String, IRI> ontologyDocuments = new HashMap<>();
        Map<IRI, InputArtifact> inputByDocument = new HashMap<>();
        for (InputArtifact input : request.inputs()) {
            IRI documentIri = IRI.create(Path.of(input.path()).toUri());
            if (inputByDocument.putIfAbsent(documentIri, input) != null) {
                throw new IllegalArgumentException("reasoner input document path is duplicated");
            }
            if (input.ontologyIris() == null) {
                throw new IllegalArgumentException("ontologyIris is required for every reasoner input");
            }
            for (String ontologyIri : input.ontologyIris()) {
                IRI previous = ontologyDocuments.putIfAbsent(ontologyIri, documentIri);
                if (previous != null && !previous.equals(documentIri)) {
                    throw new IllegalArgumentException("ontology IRI maps to more than one local document");
                }
            }
        }
        loader.getIRIMappers().add(ontologyIri -> {
            IRI mapped = ontologyDocuments.get(ontologyIri.toString());
            if (mapped == null) {
                throw new IllegalArgumentException("unmapped ontology import: " + ontologyIri);
            }
            return mapped;
        });
        for (InputArtifact input : request.inputs()) {
            IRI documentIri = IRI.create(Path.of(input.path()).toUri());
            boolean alreadyLoaded = loader.getOntologies().stream()
                    .anyMatch(ontology -> loader.getOntologyDocumentIRI(ontology).equals(documentIri));
            if (!alreadyLoaded) {
                loader.loadOntologyFromOntologyDocument(Path.of(input.path()).toFile());
            }
        }
        loader.getOntologies().forEach(ontology -> mergedAxioms.addAll(ontology.getAxioms()));
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology merged = manager.createOntology(mergedAxioms);
        String owlSignatureSha256 = writeOwlSignature(request, loader, merged);
        DatatypePolicy datatypePolicy = readAndValidateDatatypePolicy(request);
        validateDatatypeCoverage(merged, datatypePolicy);
        Set<OWLNamedIndividual> individuals = merged.getIndividualsInSignature(Imports.INCLUDED);
        long propertyCount = (long) merged.getObjectPropertiesInSignature(Imports.INCLUDED).size()
                + merged.getDataPropertiesInSignature(Imports.INCLUDED).size();
        if (individuals.size() > request.maxNamedIndividuals() || propertyCount > request.maxProperties()) {
            throw new IllegalArgumentException("reasoner request exceeds declared individual/property bounds");
        }

        OWLProfileReport profileReport = new OWL2DLProfile().checkOntology(merged);
        List<String> profileViolationSamples = profileReport.getViolations().stream()
                .map(violation -> boundedViolation(violation.toString()))
                .sorted()
                .limit(MAX_PROFILE_VIOLATION_SAMPLES)
                .toList();
        String owlProfileQualificationSha256 = writeOwlProfileQualification(
                request, loader, inputByDocument, ontologyDocuments, merged, profileReport,
                profileViolationSamples, owlSignatureSha256
        );
        if (!profileReport.isInProfile()) {
            String owlConsistencyQualificationSha256 = writeOwlConsistencyQualification(
                    request, loader, merged, owlSignatureSha256, owlProfileQualificationSha256,
                    false, false
            );
            writeReport(request, new Report(
                    5,
                    request.datasetId(),
                    request.snapshotId(),
                    REASONER_NAME,
                    REASONER_VERSION,
                    request.aggregateInputSha256(),
                    owlSignatureSha256,
                    request.datatypePolicySha256(),
                    owlProfileQualificationSha256,
                    owlConsistencyQualificationSha256,
                    OWL_PROFILE,
                    true,
                    false,
                    profileReport.getViolations().size(),
                    profileViolationSamples,
                    false,
                    false,
                    individuals.size(),
                    0,
                    false,
                    "no materialization: combined ontology is outside the OWL 2 DL profile"
            ));
            throw new IllegalArgumentException("combined ontology is not valid OWL 2 DL");
        }

        OWLReasoner reasoner = new Reasoner.ReasonerFactory().createReasoner(merged);
        try {
            boolean consistent = reasoner.isConsistent();
            String owlConsistencyQualificationSha256 = writeOwlConsistencyQualification(
                    request, loader, merged, owlSignatureSha256, owlProfileQualificationSha256,
                    true, consistent
            );
            if (consistent) {
                precomputeSupported(reasoner);
            }
            Set<OWLAxiom> closureAxioms = consistent
                    ? materializeNamedConsequences(merged, reasoner, manager.getOWLDataFactory())
                    : Set.of();
            OWLOntology closure = manager.createOntology(closureAxioms);
            Path closurePath = Path.of(request.outputClosurePath());
            Path closureParent = closurePath.getParent();
            if (closureParent != null) {
                Files.createDirectories(closureParent);
            }
            manager.saveOntology(
                    closure,
                    new NTriplesDocumentFormat(),
                    IRI.create(closurePath.toUri())
            );
            Report report = new Report(
                    5,
                    request.datasetId(),
                    request.snapshotId(),
                    REASONER_NAME,
                    REASONER_VERSION,
                    request.aggregateInputSha256(),
                    owlSignatureSha256,
                    request.datatypePolicySha256(),
                    owlProfileQualificationSha256,
                    owlConsistencyQualificationSha256,
                    OWL_PROFILE,
                    true,
                    true,
                    0,
                    List.of(),
                    true,
                    consistent,
                    individuals.size(),
                    closureAxioms.size(),
                    false,
                    "finite named-individual assertions and named class/property hierarchies; no proof DAG; not a finite closure for arbitrary OWL 2 DL query answering"
            );
            writeReport(request, report);
        } finally {
            reasoner.dispose();
        }
    }


    private static DatatypePolicy readAndValidateDatatypePolicy(Request request) throws Exception {
        Path policyPath = Path.of(request.datatypePolicyPath());
        String observed = toHex(sha256(policyPath));
        if (!observed.equals(request.datatypePolicySha256())) {
            throw new IllegalArgumentException("datatype policy SHA-256 mismatch");
        }
        DatatypePolicy policy = JSON.readValue(policyPath.toFile(), DatatypePolicy.class);
        if (policy.formatVersion() != 1
                || policy.policyId() == null || policy.policyId().isBlank()
                || !"reject_snapshot".equals(policy.unsupportedDatatypeBehavior())
                || !"reject_snapshot".equals(policy.illTypedLiteralBehavior())
                || !"preserve_source_lexical_form".equals(policy.canonicalization())
                || policy.maxLexicalBytes() <= 0
                || policy.lexicalLimits() == null
                || policy.lexicalLimits().integerDigitsMax() <= 0
                || policy.lexicalLimits().dateTimeYearDigitsMax() < 4
                || !"ascii_subset".equals(policy.lexicalLimits().xmlNameValidation())
                || policy.supportedDatatypes() == null || policy.supportedDatatypes().isEmpty()) {
            throw new IllegalArgumentException("invalid Phase 40.2 datatype policy contract");
        }
        String previous = null;
        for (SupportedDatatype datatype : policy.supportedDatatypes()) {
            if (datatype.iri() == null || datatype.iri().isBlank()
                    || datatype.lexicalSpace() == null || datatype.lexicalSpace().isBlank()) {
                throw new IllegalArgumentException("datatype policy contains an empty datatype rule");
            }
            if (previous != null && previous.compareTo(datatype.iri()) >= 0) {
                throw new IllegalArgumentException("datatype policy must be strictly sorted and duplicate-free");
            }
            previous = datatype.iri();
        }
        return policy;
    }

    private static void validateDatatypeCoverage(OWLOntology merged, DatatypePolicy policy) {
        Set<String> supported = policy.supportedDatatypes().stream()
                .map(SupportedDatatype::iri)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        List<String> unsupported = merged.getDatatypesInSignature(Imports.INCLUDED).stream()
                .map(datatype -> datatype.getIRI().toString())
                .filter(iri -> !supported.contains(iri))
                .distinct()
                .sorted()
                .toList();
        if (!unsupported.isEmpty()) {
            throw new IllegalArgumentException(
                    "merged ontology contains datatypes outside the operator policy: " + unsupported
            );
        }
    }

    private static String writeOwlProfileQualification(
            Request request,
            OWLOntologyManager loader,
            Map<IRI, InputArtifact> inputByDocument,
            Map<String, IRI> ontologyDocuments,
            OWLOntology merged,
            OWLProfileReport profileReport,
            List<String> profileViolationSamples,
            String owlSignatureSha256
    ) throws Exception {
        List<ProfileOntologyDocument> documents = new ArrayList<>();
        List<ImportResolution> imports = new ArrayList<>();
        long aboxDocuments = 0;
        for (OWLOntology ontology : loader.getOntologies()) {
            IRI documentIri = loader.getOntologyDocumentIRI(ontology);
            InputArtifact input = inputByDocument.get(documentIri);
            if (input == null) {
                throw new IllegalArgumentException("OWLAPI loaded a document outside the checksum-bound input set: " + documentIri);
            }
            if (input.ontologyIris() == null || input.ontologyIris().isEmpty()) {
                aboxDocuments += 1;
                if (!ontology.getImportsDeclarations().isEmpty()) {
                    throw new IllegalArgumentException("ABox input must not declare owl:imports without an ontology header");
                }
                continue;
            }
            String ontologyIri = ontology.getOntologyID().getOntologyIRI()
                    .orElseThrow(() -> new IllegalArgumentException("checksum-bound ontology document has no OWLAPI ontology IRI"))
                    .toString();
            String versionIri = ontology.getOntologyID().getVersionIRI().map(IRI::toString).orElse(null);
            List<String> observedAliases = new ArrayList<>();
            observedAliases.add(ontologyIri);
            if (versionIri != null) {
                observedAliases.add(versionIri);
            }
            observedAliases = observedAliases.stream().distinct().sorted().toList();
            List<String> expectedAliases = input.ontologyIris().stream().distinct().sorted().toList();
            if (!observedAliases.equals(expectedAliases)) {
                throw new IllegalArgumentException(
                        "OWLAPI ontology/version identity differs from checksum-bound preflight aliases for " + documentIri
                );
            }
            documents.add(new ProfileOntologyDocument(input.sha256(), ontologyIri, versionIri));
            for (var declaration : ontology.getImportsDeclarations()) {
                String importedIri = declaration.getIRI().toString();
                IRI resolvedDocument = ontologyDocuments.get(importedIri);
                if (resolvedDocument == null) {
                    throw new IllegalArgumentException("unmapped ontology import during Phase 40.5 qualification: " + importedIri);
                }
                InputArtifact resolvedInput = inputByDocument.get(resolvedDocument);
                if (resolvedInput == null || !loader.getOntologies().stream()
                        .anyMatch(candidate -> loader.getOntologyDocumentIRI(candidate).equals(resolvedDocument))) {
                    throw new IllegalArgumentException("owl:imports target was not loaded from its checksum-bound local document: " + importedIri);
                }
                imports.add(new ImportResolution(ontologyIri, importedIri, resolvedInput.sha256()));
            }
        }
        documents = documents.stream()
                .distinct()
                .sorted(Comparator.comparing(ProfileOntologyDocument::ontologyIri)
                        .thenComparing(document -> document.versionIri() == null ? "" : document.versionIri())
                        .thenComparing(ProfileOntologyDocument::sha256))
                .toList();
        imports = imports.stream()
                .distinct()
                .sorted(Comparator.comparing(ImportResolution::sourceOntologyIri)
                        .thenComparing(ImportResolution::importedIri)
                        .thenComparing(ImportResolution::resolvedDocumentSha256))
                .toList();
        if (documents.size() + aboxDocuments != request.inputs().size()) {
            throw new IllegalArgumentException("OWLAPI loaded ontology/ABox document counts differ from the request");
        }
        OwlProfileQualification qualification = new OwlProfileQualification(
                1,
                request.datasetId(),
                request.snapshotId(),
                request.aggregateInputSha256(),
                owlSignatureSha256,
                request.datatypePolicySha256(),
                OWL_PROFILE,
                true,
                request.inputs().size(),
                documents.size(),
                aboxDocuments,
                loader.getOntologies().size(),
                imports.size(),
                imports.size(),
                true,
                merged.getAxiomCount(),
                documents,
                imports,
                profileReport.isInProfile(),
                profileReport.getViolations().size(),
                profileViolationSamples
        );
        Path qualificationPath = Path.of(request.outputOwlProfileQualificationPath());
        Path parent = qualificationPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(qualificationPath.toFile(), qualification);
        return toHex(sha256(qualificationPath));
    }

    private static String writeOwlConsistencyQualification(
            Request request,
            OWLOntologyManager loader,
            OWLOntology merged,
            String owlSignatureSha256,
            String owlProfileQualificationSha256,
            boolean consistencyChecked,
            boolean consistent
    ) throws Exception {
        if (!consistencyChecked && consistent) {
            throw new IllegalArgumentException("unchecked consistency evidence cannot assert consistency");
        }
        OwlConsistencyQualification qualification = new OwlConsistencyQualification(
                1,
                request.datasetId(),
                request.snapshotId(),
                request.aggregateInputSha256(),
                owlSignatureSha256,
                request.datatypePolicySha256(),
                owlProfileQualificationSha256,
                OWL_PROFILE,
                true,
                REASONER_NAME,
                REASONER_VERSION,
                "OWLReasoner.isConsistent",
                request.inputs().size(),
                loader.getOntologies().size(),
                merged.getAxiomCount(),
                consistencyChecked,
                consistent,
                consistencyChecked && consistent,
                "reject_snapshot"
        );
        Path qualificationPath = Path.of(request.outputOwlConsistencyQualificationPath());
        Path parent = qualificationPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(qualificationPath.toFile(), qualification);
        return toHex(sha256(qualificationPath));
    }

    private static String writeOwlSignature(Request request, OWLOntologyManager loader, OWLOntology merged)
            throws Exception {
        List<SignatureDocument> documents = request.inputs().stream()
                .map(input -> new SignatureDocument(
                        input.sha256(),
                        input.ontologyIris().stream().distinct().sorted().toList()
                ))
                .sorted(Comparator.comparing(SignatureDocument::sha256)
                        .thenComparing(document -> String.join("\u0000", document.ontologyIris())))
                .toList();
        List<String> imports = loader.getOntologies().stream()
                .flatMap(ontology -> ontology.getImportsDeclarations().stream())
                .map(declaration -> declaration.getIRI().toString())
                .distinct()
                .sorted()
                .toList();
        OwlSignature signature = new OwlSignature(
                1,
                request.datasetId(),
                request.snapshotId(),
                request.aggregateInputSha256(),
                documents,
                imports,
                sortedEntityIris(merged.getClassesInSignature(Imports.INCLUDED)),
                sortedEntityIris(merged.getObjectPropertiesInSignature(Imports.INCLUDED)),
                sortedEntityIris(merged.getDataPropertiesInSignature(Imports.INCLUDED)),
                sortedEntityIris(merged.getAnnotationPropertiesInSignature(Imports.INCLUDED)),
                sortedEntityIris(merged.getIndividualsInSignature(Imports.INCLUDED)),
                sortedEntityIris(merged.getDatatypesInSignature(Imports.INCLUDED))
        );
        Path signaturePath = Path.of(request.outputOwlSignaturePath());
        Path parent = signaturePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(signaturePath.toFile(), signature);
        return toHex(sha256(signaturePath));
    }

    private static List<String> sortedEntityIris(Set<? extends OWLEntity> entities) {
        return entities.stream().map(entity -> entity.getIRI().toString()).distinct().sorted().toList();
    }

    private static void writeReport(Request request, Report report) throws IOException {
        Path reportPath = Path.of(request.outputReportPath());
        Path parent = reportPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(reportPath.toFile(), report);
    }

    private static String boundedViolation(String value) {
        if (value.length() <= MAX_PROFILE_VIOLATION_CHARACTERS) {
            return value;
        }
        return value.substring(0, MAX_PROFILE_VIOLATION_CHARACTERS) + "…";
    }

    private static void precomputeSupported(OWLReasoner reasoner) {
        for (InferenceType type : new InferenceType[]{
                InferenceType.CLASS_HIERARCHY,
                InferenceType.CLASS_ASSERTIONS,
                InferenceType.OBJECT_PROPERTY_HIERARCHY,
                InferenceType.DATA_PROPERTY_HIERARCHY,
                InferenceType.SAME_INDIVIDUAL
        }) {
            if (reasoner.isPrecomputed(type) || !reasoner.getPrecomputableInferenceTypes().contains(type)) {
                continue;
            }
            reasoner.precomputeInferences(type);
        }
    }

    private static Set<OWLAxiom> materializeNamedConsequences(
            OWLOntology ontology,
            OWLReasoner reasoner,
            OWLDataFactory dataFactory
    ) {
        Set<OWLAxiom> output = new HashSet<>();
        Set<OWLNamedIndividual> individuals = ontology.getIndividualsInSignature(Imports.INCLUDED);
        Set<OWLObjectProperty> objectProperties = ontology.getObjectPropertiesInSignature(Imports.INCLUDED);
        Set<OWLDataProperty> dataProperties = ontology.getDataPropertiesInSignature(Imports.INCLUDED);
        for (OWLNamedIndividual individual : individuals) {
            for (OWLClass type : reasoner.getTypes(individual, false).getFlattened()) {
                output.add(dataFactory.getOWLClassAssertionAxiom(type, individual));
            }
            for (OWLObjectProperty property : objectProperties) {
                for (OWLNamedIndividual value : reasoner.getObjectPropertyValues(individual, property).getFlattened()) {
                    output.add(dataFactory.getOWLObjectPropertyAssertionAxiom(property, individual, value));
                }
            }
            for (OWLDataProperty property : dataProperties) {
                reasoner.getDataPropertyValues(individual, property).forEach(value ->
                        output.add(dataFactory.getOWLDataPropertyAssertionAxiom(property, individual, value))
                );
            }
            reasoner.getSameIndividuals(individual).getEntities().stream()
                    .filter(other -> !other.equals(individual))
                    .forEach(other -> output.add(dataFactory.getOWLSameIndividualAxiom(individual, other)));
        }
        for (OWLClass owlClass : ontology.getClassesInSignature(Imports.INCLUDED)) {
            reasoner.getSuperClasses(owlClass, false).getFlattened().forEach(superClass ->
                    output.add(dataFactory.getOWLSubClassOfAxiom(owlClass, superClass))
            );
            reasoner.getEquivalentClasses(owlClass).getEntities().stream()
                    .filter(equivalent -> !equivalent.equals(owlClass))
                    .forEach(equivalent -> output.add(dataFactory.getOWLEquivalentClassesAxiom(owlClass, equivalent)));
        }
        for (OWLObjectProperty property : objectProperties) {
            for (OWLObjectPropertyExpression superProperty : reasoner.getSuperObjectProperties(property, false).getFlattened()) {
                output.add(dataFactory.getOWLSubObjectPropertyOfAxiom(property, superProperty));
            }
        }
        for (OWLDataProperty property : dataProperties) {
            reasoner.getSuperDataProperties(property, false).getFlattened().forEach(superProperty ->
                    output.add(dataFactory.getOWLSubDataPropertyOfAxiom(property, superProperty))
            );
        }
        return output;
    }

    private static void verifyInputs(Request request) throws Exception {
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        for (InputArtifact input : request.inputs()) {
            Path inputPath = Path.of(input.path());
            byte[] observed = sha256(inputPath);
            String observedHex = toHex(observed);
            if (!observedHex.equals(input.sha256())) {
                throw new IllegalArgumentException("input SHA-256 mismatch: " + inputPath);
            }
            aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(observed.length).array());
            aggregate.update(observed);
        }
        if (!toHex(aggregate.digest()).equals(request.aggregateInputSha256())) {
            throw new IllegalArgumentException("aggregate input SHA-256 mismatch");
        }
    }

    private static byte[] sha256(Path path) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(path)) {
            byte[] buffer = new byte[1024 * 1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return digest.digest();
    }

    private static String toHex(byte[] bytes) {
        StringBuilder output = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            output.append(String.format("%02x", value & 0xff));
        }
        return output.toString();
    }

    public record InputArtifact(String path, String sha256, List<String> ontologyIris) {
    }

    public record Request(
            int formatVersion,
            UUID datasetId,
            UUID snapshotId,
            List<InputArtifact> inputs,
            String aggregateInputSha256,
            String outputClosurePath,
            String outputReportPath,
            String outputOwlSignaturePath,
            String outputOwlProfileQualificationPath,
            String outputOwlConsistencyQualificationPath,
            String datatypePolicyPath,
            String datatypePolicySha256,
            long maxNamedIndividuals,
            long maxProperties
    ) {
    }


    public record SignatureDocument(String sha256, List<String> ontologyIris) {
    }

    public record OwlSignature(
            int formatVersion,
            UUID datasetId,
            UUID snapshotId,
            String aggregateInputSha256,
            List<SignatureDocument> ontologyDocuments,
            List<String> imports,
            List<String> classes,
            List<String> objectProperties,
            List<String> dataProperties,
            List<String> annotationProperties,
            List<String> namedIndividuals,
            List<String> datatypes
    ) {
    }

    public record LexicalLimits(
            int integerDigitsMax,
            int dateTimeYearDigitsMax,
            String xmlNameValidation
    ) {
    }

    public record SupportedDatatype(String iri, String lexicalSpace) {
    }

    public record DatatypePolicy(
            int formatVersion,
            String policyId,
            String unsupportedDatatypeBehavior,
            String illTypedLiteralBehavior,
            String canonicalization,
            long maxLexicalBytes,
            LexicalLimits lexicalLimits,
            List<SupportedDatatype> supportedDatatypes
    ) {
    }

    public record ProfileOntologyDocument(String sha256, String ontologyIri, String versionIri) {
    }

    public record ImportResolution(String sourceOntologyIri, String importedIri, String resolvedDocumentSha256) {
    }

    public record OwlProfileQualification(
            int formatVersion,
            UUID datasetId,
            UUID snapshotId,
            String aggregateInputSha256,
            String owlSignatureSha256,
            String datatypePolicySha256,
            String owlProfile,
            boolean directSemantics,
            long inputDocumentCount,
            long ontologyDocumentCount,
            long aboxDocumentCount,
            long loadedOntologyCount,
            long importDeclarationCount,
            long resolvedImportCount,
            boolean completeLocalImportClosure,
            long mergedAxiomCount,
            List<ProfileOntologyDocument> ontologyDocuments,
            List<ImportResolution> importResolutions,
            boolean profileValid,
            long profileViolationCount,
            List<String> profileViolationSamples
    ) {
    }

    public record OwlConsistencyQualification(
            int formatVersion,
            UUID datasetId,
            UUID snapshotId,
            String aggregateInputSha256,
            String owlSignatureSha256,
            String datatypePolicySha256,
            String owlProfileQualificationSha256,
            String owlProfile,
            boolean directSemantics,
            String reasonerName,
            String reasonerVersion,
            String consistencyMethod,
            long inputDocumentCount,
            long loadedOntologyCount,
            long mergedAxiomCount,
            boolean consistencyChecked,
            boolean consistent,
            boolean publicationPermitted,
            String inconsistentOntologyHandling
    ) {
    }

    public record Report(
            int formatVersion,
            UUID datasetId,
            UUID snapshotId,
            String reasonerName,
            String reasonerVersion,
            String aggregateInputSha256,
            String owlSignatureSha256,
            String datatypePolicySha256,
            String owlProfileQualificationSha256,
            String owlConsistencyQualificationSha256,
            String owlProfile,
            boolean directSemantics,
            boolean profileValid,
            long profileViolationCount,
            List<String> profileViolationSamples,
            boolean consistencyChecked,
            boolean consistent,
            long namedIndividualCount,
            long emittedAxiomCount,
            boolean proofDagAvailable,
            String materializationScope
    ) {
    }
}
