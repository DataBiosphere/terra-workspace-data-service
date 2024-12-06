package org.databiosphere.workspacedataservice.controller;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.expressions.ExpressionService;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsRequestServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayRequestServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayResponseServerModel;
import org.databiosphere.workspacedataservice.generated.ExpressionEvaluationsForRecordServerModel;
import org.databiosphere.workspacedataservice.generated.NamedExpressionServerModel;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordControllerExpressionsTest extends MockMvcTestBase {
  @MockitoBean private ExpressionService expressionService;
  @MockitoBean private PermissionService permissionService;

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
        new EvaluateExpressionsRequestServerModel()
            .expressions(
                expressionsByName.entrySet().stream()
                    .map(
                        e ->
                            new NamedExpressionServerModel()
                                .name(e.getKey())
                                .expression(e.getValue()))
                    .toList());
    var collectionId = CollectionId.of(UUID.randomUUID());
    var recordType = "recordType";
    var recordId = UUID.randomUUID().toString();

    doNothing().when(permissionService).requireReadPermission(collectionId);
    var expressionEvaluation =
        new EvaluateExpressionsResponseServerModel(Map.of(expressionName, value));
    when(expressionService.evaluateExpressions(
            collectionId, RecordType.valueOf(recordType), recordId, expressionsByName))
        .thenReturn(expressionEvaluation);

    mockMvc
        .perform(
            post(
                    "/records/v1/{collectionId}/{recordType}/{recordId}/evaluateExpressions",
                    collectionId,
                    recordType,
                    recordId)
                .content(toJson(request))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(toJson(expressionEvaluation)));

    verify(permissionService).requireReadPermission(collectionId);
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
        new EvaluateExpressionsWithArrayRequestServerModel(
            expressionsByName.entrySet().stream()
                .map(
                    e -> new NamedExpressionServerModel().name(e.getKey()).expression(e.getValue()))
                .toList(),
            relationExpression,
            pageSize,
            offset);
    var collectionId = CollectionId.of(UUID.randomUUID());
    var recordType = "recordType";
    var recordId = UUID.randomUUID().toString();

    doNothing().when(permissionService).requireReadPermission(collectionId);
    var expressionEvaluation =
        new ExpressionEvaluationsForRecordServerModel("recordId", Map.of(expressionName, value));
    var expectedResponse =
        new EvaluateExpressionsWithArrayResponseServerModel(List.of(expressionEvaluation), false);
    when(expressionService.evaluateExpressionsWithRelationArray(
            collectionId,
            RecordType.valueOf(recordType),
            recordId,
            relationExpression,
            expressionsByName,
            pageSize,
            offset))
        .thenReturn(expectedResponse);

    mockMvc
        .perform(
            post(
                    "/records/v1/{collectionId}/{recordType}/{recordId}/evaluateExpressionsWithArray",
                    collectionId,
                    recordType,
                    recordId)
                .content(toJson(request))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(content().json(toJson(expectedResponse)));

    verify(permissionService).requireReadPermission(collectionId);
  }
}
