package io.ngkg.reasoner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void materializesNamedIndividualSuperclassType() throws Exception {
        Path ontology = temporaryDirectory.resolve("input.ttl");
        Files.writeString(ontology, """
                @prefix ex: <https://example.test/> .
                @prefix owl: <http://www.w3.org/2002/07/owl#> .
                @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                ex:A a owl:Class ; rdfs:subClassOf ex:B .
                ex:B a owl:Class .
                ex:x a ex:A .
                """);
        byte[] inputHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(ontology));
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(inputHash.length).array());
        aggregate.update(inputHash);
        Path closure = temporaryDirectory.resolve("closure.nt");
        Path report = temporaryDirectory.resolve("report.json");
        Path signature = temporaryDirectory.resolve("owl-signature.json");
        Path datatypePolicy = temporaryDirectory.resolve("datatype-policy.json");
        Files.writeString(datatypePolicy, minimalDatatypePolicy("http://www.w3.org/2001/XMLSchema#string", "string"));
        String datatypePolicySha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(datatypePolicy))
        );
        Path request = temporaryDirectory.resolve("request.json");
        UUID datasetId = UUID.fromString("4d2e1a82-c2bc-536a-a809-fda7643ef1f7");
        UUID snapshotId = UUID.fromString("91054ecb-2f68-5a63-b31a-137333c64a7c");
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("formatVersion", 4);
        value.put("datasetId", datasetId);
        value.put("snapshotId", snapshotId);
        value.put("inputs", List.of(Map.of(
                "path", ontology.toString(),
                "sha256", HexFormat.of().formatHex(inputHash),
                "ontologyIris", List.of()
        )));
        value.put("aggregateInputSha256", HexFormat.of().formatHex(aggregate.digest()));
        value.put("outputClosurePath", closure.toString());
        value.put("outputReportPath", report.toString());
        value.put("outputOwlSignaturePath", signature.toString());
        Path profileQualification = temporaryDirectory.resolve("owl-profile-qualification.json");
        Path consistencyQualification = temporaryDirectory.resolve("owl-consistency-qualification.json");
        value.put("outputOwlProfileQualificationPath", profileQualification.toString());
        value.put("outputOwlConsistencyQualificationPath", consistencyQualification.toString());
        value.put("datatypePolicyPath", datatypePolicy.toString());
        value.put("datatypePolicySha256", datatypePolicySha256);
        value.put("maxNamedIndividuals", 100);
        value.put("maxProperties", 100);
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(request.toFile(), value);

        Main.run(request);

        String closureText = Files.readString(closure);
        JsonNode reportJson = new ObjectMapper().readTree(report.toFile());
        JsonNode signatureJson = new ObjectMapper().readTree(signature.toFile());
        String observedSignatureHash = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(signature))
        );
        assertTrue(reportJson.path("consistent").asBoolean());
        assertTrue(reportJson.path("owlSignatureSha256").asText().equals(observedSignatureHash));
        assertTrue(reportJson.path("datatypePolicySha256").asText().equals(datatypePolicySha256));
        String observedQualificationHash = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(profileQualification))
        );
        assertTrue(reportJson.path("owlProfileQualificationSha256").asText().equals(observedQualificationHash));
        String observedConsistencyQualificationHash = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(consistencyQualification))
        );
        assertTrue(reportJson.path("owlConsistencyQualificationSha256").asText().equals(observedConsistencyQualificationHash));
        JsonNode consistencyJson = new ObjectMapper().readTree(consistencyQualification.toFile());
        assertTrue(consistencyJson.path("consistencyChecked").asBoolean());
        assertTrue(consistencyJson.path("consistent").asBoolean());
        assertTrue(consistencyJson.path("publicationPermitted").asBoolean());
        JsonNode qualificationJson = new ObjectMapper().readTree(profileQualification.toFile());
        assertTrue(qualificationJson.path("completeLocalImportClosure").asBoolean());
        assertTrue(qualificationJson.path("profileValid").asBoolean());
        assertTrue(signatureJson.path("classes").toString().contains("https://example.test/A"));
        assertTrue(signatureJson.path("classes").toString().contains("https://example.test/B"));
        assertTrue(signatureJson.path("namedIndividuals").toString().contains("https://example.test/x"));
        assertTrue(closureText.contains("https://example.test/B"));
        assertTrue(closureText.contains("https://example.test/x"));
    }
    @Test
    void rejectsMergedOntologyDatatypeOutsidePolicy() throws Exception {
        Path ontology = temporaryDirectory.resolve("unsupported.ttl");
        Files.writeString(ontology, """
                @prefix ex: <https://example.test/> .
                @prefix owl: <http://www.w3.org/2002/07/owl#> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                ex:p a owl:DatatypeProperty .
                ex:x ex:p "42"^^xsd:integer .
                """);
        byte[] inputHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(ontology));
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(inputHash.length).array());
        aggregate.update(inputHash);
        Path policy = temporaryDirectory.resolve("restricted-policy.json");
        Files.writeString(policy, minimalDatatypePolicy("http://www.w3.org/2001/XMLSchema#string", "string"));
        String policyHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(policy)));
        Map<String, Object> requestValue = new LinkedHashMap<>();
        requestValue.put("formatVersion", 4);
        requestValue.put("datasetId", UUID.randomUUID());
        requestValue.put("snapshotId", UUID.randomUUID());
        requestValue.put("inputs", List.of(Map.of("path", ontology.toString(), "sha256", HexFormat.of().formatHex(inputHash), "ontologyIris", List.of())));
        requestValue.put("aggregateInputSha256", HexFormat.of().formatHex(aggregate.digest()));
        requestValue.put("outputClosurePath", temporaryDirectory.resolve("unsupported-closure.nt").toString());
        requestValue.put("outputReportPath", temporaryDirectory.resolve("unsupported-report.json").toString());
        requestValue.put("outputOwlSignaturePath", temporaryDirectory.resolve("unsupported-signature.json").toString());
        requestValue.put("outputOwlProfileQualificationPath", temporaryDirectory.resolve("unsupported-profile-qualification.json").toString());
        requestValue.put("outputOwlConsistencyQualificationPath", temporaryDirectory.resolve("unsupported-consistency-qualification.json").toString());
        requestValue.put("datatypePolicyPath", policy.toString());
        requestValue.put("datatypePolicySha256", policyHash);
        requestValue.put("maxNamedIndividuals", 100);
        requestValue.put("maxProperties", 100);
        Path request = temporaryDirectory.resolve("unsupported-request.json");
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(request.toFile(), requestValue);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> Main.run(request));
        assertTrue(error.getMessage().contains("outside the operator policy"));
    }

    @Test
    void resolvesVersionIriImportIntoChecksumBoundLocalClosure() throws Exception {
        Path rootOntology = temporaryDirectory.resolve("root.ttl");
        Path childOntology = temporaryDirectory.resolve("child.ttl");
        Files.writeString(rootOntology, """
                @prefix owl: <http://www.w3.org/2002/07/owl#> .
                <https://example.test/root> a owl:Ontology ;
                    owl:imports <https://example.test/child/2026> .
                """);
        Files.writeString(childOntology, """
                @prefix owl: <http://www.w3.org/2002/07/owl#> .
                <https://example.test/child> a owl:Ontology ;
                    owl:versionIRI <https://example.test/child/2026> .
                <https://example.test/C> a owl:Class .
                """);
        byte[] rootHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(rootOntology));
        byte[] childHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(childOntology));
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        for (byte[] digest : List.of(rootHash, childHash)) {
            aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(digest.length).array());
            aggregate.update(digest);
        }
        Path policy = temporaryDirectory.resolve("import-policy.json");
        Files.writeString(policy, minimalDatatypePolicy("http://www.w3.org/2001/XMLSchema#string", "string"));
        String policyHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(policy)));
        Path qualification = temporaryDirectory.resolve("import-profile-qualification.json");
        Path report = temporaryDirectory.resolve("import-report.json");
        Map<String, Object> requestValue = new LinkedHashMap<>();
        requestValue.put("formatVersion", 4);
        requestValue.put("datasetId", UUID.randomUUID());
        requestValue.put("snapshotId", UUID.randomUUID());
        requestValue.put("inputs", List.of(
                Map.of("path", rootOntology.toString(), "sha256", HexFormat.of().formatHex(rootHash), "ontologyIris", List.of("https://example.test/root")),
                Map.of("path", childOntology.toString(), "sha256", HexFormat.of().formatHex(childHash), "ontologyIris", List.of("https://example.test/child", "https://example.test/child/2026"))
        ));
        requestValue.put("aggregateInputSha256", HexFormat.of().formatHex(aggregate.digest()));
        requestValue.put("outputClosurePath", temporaryDirectory.resolve("import-closure.nt").toString());
        requestValue.put("outputReportPath", report.toString());
        requestValue.put("outputOwlSignaturePath", temporaryDirectory.resolve("import-signature.json").toString());
        requestValue.put("outputOwlProfileQualificationPath", qualification.toString());
        Path consistencyQualification = temporaryDirectory.resolve("import-consistency-qualification.json");
        requestValue.put("outputOwlConsistencyQualificationPath", consistencyQualification.toString());
        requestValue.put("datatypePolicyPath", policy.toString());
        requestValue.put("datatypePolicySha256", policyHash);
        requestValue.put("maxNamedIndividuals", 100);
        requestValue.put("maxProperties", 100);
        Path request = temporaryDirectory.resolve("import-request.json");
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(request.toFile(), requestValue);

        Main.run(request);

        JsonNode evidence = new ObjectMapper().readTree(qualification.toFile());
        assertTrue(evidence.path("completeLocalImportClosure").asBoolean());
        assertTrue(evidence.path("ontologyDocumentCount").asInt() == 2);
        assertTrue(evidence.path("importDeclarationCount").asInt() == 1);
        assertTrue(evidence.path("resolvedImportCount").asInt() == 1);
        assertTrue(evidence.path("importResolutions").toString().contains("https://example.test/child/2026"));
        JsonNode reportJson = new ObjectMapper().readTree(report.toFile());
        String qualificationHash = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(qualification))
        );
        assertTrue(reportJson.path("owlProfileQualificationSha256").asText().equals(qualificationHash));
        String consistencyHash = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(consistencyQualification))
        );
        assertTrue(reportJson.path("owlConsistencyQualificationSha256").asText().equals(consistencyHash));
    }

    @Test
    void emitsFailClosedEvidenceForGloballyInconsistentMergedOntology() throws Exception {
        Path ontology = temporaryDirectory.resolve("inconsistent.ttl");
        Files.writeString(ontology, """
                @prefix ex: <https://example.test/> .
                @prefix owl: <http://www.w3.org/2002/07/owl#> .
                ex:A a owl:Class ; owl:disjointWith ex:B .
                ex:B a owl:Class .
                ex:x a ex:A, ex:B .
                """);
        byte[] inputHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(ontology));
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(inputHash.length).array());
        aggregate.update(inputHash);
        Path policy = temporaryDirectory.resolve("inconsistent-policy.json");
        Files.writeString(policy, minimalDatatypePolicy("http://www.w3.org/2001/XMLSchema#string", "string"));
        String policyHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(policy)));
        Path report = temporaryDirectory.resolve("inconsistent-report.json");
        Path consistency = temporaryDirectory.resolve("inconsistent-consistency.json");
        Map<String, Object> requestValue = new LinkedHashMap<>();
        requestValue.put("formatVersion", 4);
        requestValue.put("datasetId", UUID.randomUUID());
        requestValue.put("snapshotId", UUID.randomUUID());
        requestValue.put("inputs", List.of(Map.of("path", ontology.toString(), "sha256", HexFormat.of().formatHex(inputHash), "ontologyIris", List.of())));
        requestValue.put("aggregateInputSha256", HexFormat.of().formatHex(aggregate.digest()));
        requestValue.put("outputClosurePath", temporaryDirectory.resolve("inconsistent-closure.nt").toString());
        requestValue.put("outputReportPath", report.toString());
        requestValue.put("outputOwlSignaturePath", temporaryDirectory.resolve("inconsistent-signature.json").toString());
        requestValue.put("outputOwlProfileQualificationPath", temporaryDirectory.resolve("inconsistent-profile.json").toString());
        requestValue.put("outputOwlConsistencyQualificationPath", consistency.toString());
        requestValue.put("datatypePolicyPath", policy.toString());
        requestValue.put("datatypePolicySha256", policyHash);
        requestValue.put("maxNamedIndividuals", 100);
        requestValue.put("maxProperties", 100);
        Path request = temporaryDirectory.resolve("inconsistent-request.json");
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(request.toFile(), requestValue);

        Main.run(request);

        JsonNode evidence = new ObjectMapper().readTree(consistency.toFile());
        JsonNode reportJson = new ObjectMapper().readTree(report.toFile());
        assertTrue(evidence.path("consistencyChecked").asBoolean());
        assertTrue(!evidence.path("consistent").asBoolean());
        assertTrue(!evidence.path("publicationPermitted").asBoolean());
        assertTrue(reportJson.path("consistencyChecked").asBoolean());
        assertTrue(!reportJson.path("consistent").asBoolean());
    }

    private static String minimalDatatypePolicy(String iri, String lexicalSpace) throws Exception {
        Map<String, Object> policy = new LinkedHashMap<>();
        policy.put("formatVersion", 1);
        policy.put("policyId", "ngkg-owl2-direct-datatype-policy-v1");
        policy.put("unsupportedDatatypeBehavior", "reject_snapshot");
        policy.put("illTypedLiteralBehavior", "reject_snapshot");
        policy.put("canonicalization", "preserve_source_lexical_form");
        policy.put("maxLexicalBytes", 1024);
        policy.put("lexicalLimits", Map.of("integerDigitsMax", 128, "dateTimeYearDigitsMax", 18, "xmlNameValidation", "ascii_subset"));
        policy.put("supportedDatatypes", List.of(Map.of("iri", iri, "lexicalSpace", lexicalSpace)));
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(policy);
    }


    @Test
    void exactDirectBgpEntailsSuperclassBinding() throws Exception {
        Path ontology = temporaryDirectory.resolve("direct-input.ttl");
        Files.writeString(ontology, """
                @prefix ex: <https://example.test/> .
                @prefix owl: <http://www.w3.org/2002/07/owl#> .
                @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                ex:A a owl:Class ; rdfs:subClassOf ex:B .
                ex:B a owl:Class .
                ex:alice a owl:NamedIndividual, ex:A .
                """);
        byte[] inputHash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(ontology));
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        aggregate.update(ByteBuffer.allocate(Long.BYTES).putLong(inputHash.length).array());
        aggregate.update(inputHash);
        Path output = temporaryDirectory.resolve("direct-result.json");
        Map<String,Object> request = new LinkedHashMap<>();
        request.put("formatVersion", 1);
        request.put("datasetId", UUID.fromString("4d2e1a82-c2bc-536a-a809-fda7643ef1f7"));
        request.put("snapshotId", UUID.fromString("91054ecb-2f68-5a63-b31a-137333c64a7c"));
        request.put("querySha256", "11".repeat(32));
        request.put("sparqlAlgebraSha256", "22".repeat(32));
        request.put("bgpSha256", "33".repeat(32));
        request.put("activeDatasetSha256", "44".repeat(32));
        request.put("authorizedGraphSetSha256", "55".repeat(32));
        request.put("owlSignatureSha256", "66".repeat(32));
        request.put("datatypePolicySha256", "77".repeat(32));
        request.put("owlProfileQualificationSha256", "88".repeat(32));
        request.put("owlConsistencyQualificationSha256", "99".repeat(32));
        request.put("engine", "hermit-grounded-owl2dl-isentailed-v1");
        request.put("inputs", List.of(Map.of("path", ontology.toString(), "sha256", HexFormat.of().formatHex(inputHash), "ontologyIris", List.of())));
        request.put("aggregateInputSha256", HexFormat.of().formatHex(aggregate.digest()));
        request.put("template", Map.of(
                "ordinal", 0,
                "bgpSha256", "33".repeat(32),
                "graphScope", Map.of("scope", "default"),
                "variables", List.of(Map.of("name", "x", "role", "named-individual")),
                "triples", List.of(Map.of(
                        "subject", Map.of("termType", "variable", "name", "x"),
                        "predicate", Map.of("termType", "iri", "value", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                        "object", Map.of("termType", "iri", "value", "https://example.test/B")
                ))
        ));
        request.put("partition", Map.of("index", 0, "count", 1));
        request.put("maxCandidateBindings", 100);
        request.put("maxPartitionCandidates", 100);
        request.put("maxGroundedAxiomsPerCandidate", 100);
        request.put("maxGroundedRdfBytesPerCandidate", 1048576);
        request.put("outputPath", output.toString());
        Path requestPath = temporaryDirectory.resolve("direct-request.json");
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(requestPath.toFile(), request);
        DirectBgpExecutor.run(requestPath);
        JsonNode result = new ObjectMapper().readTree(output.toFile());
        assertTrue(result.path("complete").asBoolean());
        assertTrue(result.path("candidateBindingCount").asLong() == 1);
        assertTrue(result.path("entailedCandidateCount").asLong() == 1);
        JsonNode evidence = result.path("entailed").get(0);
        assertTrue(evidence.path("bindings").path("x").path("value").asText().equals("https://example.test/alice"));
        assertTrue(evidence.path("groundedRdfSha256").asText().matches("[0-9a-f]{64}"));
        assertTrue(evidence.path("logicalAxiomsSha256").asText().matches("[0-9a-f]{64}"));
        assertTrue(evidence.path("logicalAxiomCount").asLong() > 0);
        assertTrue(result.path("adapterVersion").asText().equals("40.9"));
    }

}
