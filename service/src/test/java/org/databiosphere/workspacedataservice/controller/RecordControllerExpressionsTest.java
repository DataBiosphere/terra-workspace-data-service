package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.expressions.ExpressionService;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.EvaluateExpressionsRequest;
import org.databiosphere.workspacedataservice.shared.model.EvaluateExpressionsResponse;
import org.databiosphere.workspacedataservice.shared.model.EvaluateExpressionsWithArrayRequest;
import org.databiosphere.workspacedataservice.shared.model.EvaluateExpressionsWithArrayResponse;
import org.databiosphere.workspacedataservice.shared.model.NamedExpression;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordControllerExpressionsTest extends MockMvcTestBase {
  @MockBean private ExpressionService expressionService;
  @MockBean private PermissionService permissionService;

  public Stream<Arguments> testEvaluateExpressionValues() {
    return Stream.of(
        Arguments.of(mapper.createObjectNode().put("key", "value").put("key2", "value2")),
        Arguments.of(mapper.createArrayNode().add("value").add("value2")),
        Arguments.of(IntNode.valueOf(42)),
        Arguments.of(TextNode.valueOf("text")));
  }

  @ParameterizedTest
  @MethodSource("testEvaluateExpressionValues")
  void testEvaluateExpressions(JsonNode value) throws Exception {
    var expressionName = "expr_name";
    var expressionsByName = Map.of(expressionName, "this.attribute");
    var request =
        new EvaluateExpressionsRequest(
            expressionsByName.entrySet().stream()
                .map(e -> new NamedExpression(e.getKey(), e.getValue()))
                .toList());
    var collectionId = UUID.randomUUID();
    var recordType = "recordType";
    var recordId = UUID.randomUUID().toString();

    doNothing().when(permissionService).requireReadPermission(CollectionId.of(collectionId));
    var expressionEvaluation = Map.of(expressionName, value);
    when(expressionService.evaluateExpressions(
            collectionId, VERSION, RecordType.valueOf(recordType), recordId, expressionsByName))
        .thenReturn(expressionEvaluation);
    var expectedResponse = new EvaluateExpressionsResponse(expressionEvaluation);

    mockMvc
        .perform(
            post(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}/evaluateExpressions",
                    collectionId,
                    VERSION,
                    recordType,
                    recordId)
                .content(toJson(request))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(toJson(expectedResponse)));

    verify(permissionService).requireReadPermission(CollectionId.of(collectionId));
  }

  @ParameterizedTest
  @MethodSource("testEvaluateExpressionValues")
  void testEvaluateExpressionsWithArray(JsonNode value) throws Exception {
    var expressionName = "expr_name";
    var expressionsByName = Map.of(expressionName, "this.attribute");
    var relationExpression = "this.relation";
    var pageSize = 10;
    var offset = 0;
    var request =
        new EvaluateExpressionsWithArrayRequest(
            relationExpression,
            expressionsByName.entrySet().stream()
                .map(e -> new NamedExpression(e.getKey(), e.getValue()))
                .toList(),
            offset,
            pageSize);
    var collectionId = UUID.randomUUID();
    var recordType = "recordType";
    var recordId = UUID.randomUUID().toString();

    doNothing().when(permissionService).requireReadPermission(CollectionId.of(collectionId));
    var expressionEvaluation = Map.of("recordId", Map.of(expressionName, value));
    when(expressionService.evaluateExpressionsWithRelationArray(
            collectionId,
            VERSION,
            RecordType.valueOf(recordType),
            recordId,
            relationExpression,
            expressionsByName,
            pageSize,
            offset))
        .thenReturn(expressionEvaluation);
    var expectedResponse = EvaluateExpressionsWithArrayResponse.of(expressionEvaluation);

    mockMvc
        .perform(
            post(
                    "/{instanceId}/records/{version}/{recordType}/{recordId}/evaluateExpressionsWithArray",
                    collectionId,
                    VERSION,
                    recordType,
                    recordId)
                .content(toJson(request))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(toJson(expectedResponse)));

    verify(permissionService).requireReadPermission(CollectionId.of(collectionId));
  }
}
