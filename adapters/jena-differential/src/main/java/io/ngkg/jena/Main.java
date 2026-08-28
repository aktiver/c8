package io.ngkg.jena;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.util.FmtUtils;

/** Bounded Apache Jena differential driver for SPARQL syntax and SELECT/ASK results. */
public final class Main {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private static final String ENGINE = "apache-jena";
  private static final String VERSION = "6.2.0";

  private Main() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 1) {
      throw new IllegalArgumentException("usage: ngkg-jena-differential request.json");
    }
    JsonNode request = MAPPER.readTree(Path.of(arguments[0]).toFile());
    String caseId = requiredText(request, "caseId");
    JsonNode descriptor = request.required("descriptor");
    String operation = requiredText(descriptor, "operation");
    ObjectNode output;
    try {
      output = switch (operation) {
        case "sparql-syntax" -> syntax(caseId, descriptor);
        case "sparql-select-ask" -> selectOrAsk(caseId, descriptor);
        case "trig-syntax" -> trigSyntax(caseId, descriptor);
        default -> throw new StableFailure("UNSUPPORTED_CASE");
      };
    } catch (StableFailure failure) {
      output = failure(caseId, failure.errorClass);
    } catch (RuntimeException | IOException failure) {
      output = failure(caseId, classify(failure));
    }
    System.out.write(MAPPER.writeValueAsBytes(output));
  }

  private static ObjectNode syntax(String caseId, JsonNode descriptor) throws IOException {
    String queryText = Files.readString(realFile(descriptor, "queryPath"), StandardCharsets.UTF_8);
    boolean expected = descriptor.required("expectedParseSuccess").asBoolean();
    boolean accepted;
    try {
      QueryFactory.create(queryText, descriptor.path("baseIri").asText(null), Syntax.syntaxSPARQL_11);
      accepted = true;
    } catch (RuntimeException failure) {
      accepted = false;
    }
    if (accepted != expected) {
      throw new StableFailure("SPARQL_SYNTAX_MISMATCH");
    }
    return success(caseId, sha256(accepted ? "accepted" : "rejected"));
  }

  private static ObjectNode trigSyntax(String caseId, JsonNode descriptor) throws IOException {
    boolean expected = descriptor.required("expectedParseSuccess").asBoolean();
    boolean accepted;
    try {
      Dataset dataset = DatasetFactory.createTxnMem();
      RDFDataMgr.read(dataset, realFile(descriptor, "datasetPath").toString(), Lang.TRIG);
      accepted = true;
    } catch (RuntimeException failure) {
      accepted = false;
    }
    if (accepted != expected) {
      throw new StableFailure("TRIG_SYNTAX_MISMATCH");
    }
    return success(caseId, sha256(accepted ? "accepted" : "rejected"));
  }

  private static ObjectNode selectOrAsk(String caseId, JsonNode descriptor) throws IOException {
    Dataset dataset = DatasetFactory.createTxnMem();
    RDFDataMgr.read(dataset, realFile(descriptor, "datasetPath").toString(), Lang.TRIG);
    String queryText = Files.readString(realFile(descriptor, "queryPath"), StandardCharsets.UTF_8);
    Query query;
    try {
      query = QueryFactory.create(queryText, descriptor.path("baseIri").asText(null), Syntax.syntaxSPARQL_11);
    } catch (RuntimeException failure) {
      throw new StableFailure("MALFORMED_QUERY");
    }
    try (QueryExecution execution = QueryExecution.dataset(dataset).query(query).build()) {
      if (query.isAskType()) {
        return success(caseId, sha256(Boolean.toString(execution.execAsk())));
      }
      if (!query.isSelectType()) {
        throw new StableFailure("UNSUPPORTED_QUERY_FORM");
      }
      ResultSet result = execution.execSelect();
      List<String> variables = new ArrayList<>(result.getResultVars());
      Collections.sort(variables);
      List<String> rows = new ArrayList<>();
      while (result.hasNext()) {
        QuerySolution solution = result.nextSolution();
        ObjectNode row = MAPPER.createObjectNode();
        for (String variable : variables) {
          if (solution.contains(variable)) {
            row.put(variable, FmtUtils.stringForNode(solution.get(variable).asNode()));
          }
        }
        rows.add(MAPPER.writeValueAsString(row));
      }
      if (!query.hasOrderBy()) {
        Collections.sort(rows);
      }
      ObjectNode canonical = MAPPER.createObjectNode();
      ArrayNode variableArray = canonical.putArray("variables");
      variables.forEach(variableArray::add);
      ArrayNode rowArray = canonical.putArray("rows");
      rows.forEach(rowArray::add);
      return success(caseId, sha256(MAPPER.writeValueAsString(canonical)));
    }
  }

  private static Path realFile(JsonNode descriptor, String field) throws IOException {
    Path path = Path.of(requiredText(descriptor, field));
    if (!path.isAbsolute()) {
      throw new StableFailure("UNSAFE_INPUT_PATH");
    }
    Path real = path.toRealPath();
    if (!Files.isRegularFile(real) || Files.isSymbolicLink(path)) {
      throw new StableFailure("UNSAFE_INPUT_PATH");
    }
    return real;
  }

  private static String requiredText(JsonNode node, String field) {
    JsonNode value = node.required(field);
    if (!value.isTextual() || value.textValue().isEmpty()) {
      throw new StableFailure("INVALID_DRIVER_REQUEST");
    }
    return value.textValue();
  }

  private static ObjectNode success(String caseId, String resultSha256) {
    return envelope(caseId).put("outcome", "success").put("resultSha256", resultSha256).putNull("errorClass").put("complete", true);
  }

  private static ObjectNode failure(String caseId, String errorClass) {
    return envelope(caseId).put("outcome", "failure").putNull("resultSha256").put("errorClass", errorClass).put("complete", true);
  }

  private static ObjectNode envelope(String caseId) {
    return MAPPER.createObjectNode().put("formatVersion", 1).put("engine", ENGINE).put("engineVersion", VERSION).put("caseId", caseId);
  }

  private static String sha256(String value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static String classify(Exception failure) {
    String name = failure.getClass().getSimpleName().toUpperCase();
    return name.contains("QUERY") || name.contains("PARSE") ? "MALFORMED_QUERY" : "ORACLE_EXECUTION_FAILED";
  }

  private static final class StableFailure extends RuntimeException {
    private final String errorClass;
    private StableFailure(String errorClass) { this.errorClass = errorClass; }
  }
}
