package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.expressions.ExpressionService;
import org.databiosphere.workspacedataservice.generated.DeleteRecordsRequestServerModel;
import org.databiosphere.workspacedataservice.generated.DeleteRecordsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsRequestServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayRequestServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayResponseServerModel;
import org.databiosphere.workspacedataservice.generated.NamedExpressionServerModel;
import org.databiosphere.workspacedataservice.generated.RecordApi;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@ConditionalOnProperty(name = "controlPlanePreview", havingValue = "on")
@RestController
public class RecordV1Controller implements RecordApi {

  private final RecordOrchestratorService recordOrchestratorService;
  private final PermissionService permissionService;
  private final ExpressionService expressionService;

  public RecordV1Controller(
      RecordOrchestratorService recordOrchestratorService,
      PermissionService permissionService,
      ExpressionService expressionService) {
    this.recordOrchestratorService = recordOrchestratorService;
    this.permissionService = permissionService;
    this.expressionService = expressionService;
  }

  @Override
  public ResponseEntity<DeleteRecordsResponseServerModel> deleteRecords(
      UUID collectionId,
      String recordType,
      DeleteRecordsRequestServerModel deleteRecordsRequestServerModel) {
    permissionService.requireWritePermission(CollectionId.of(collectionId));

    DeleteRecordsResponseServerModel response =
        recordOrchestratorService.deleteRecords(
            CollectionId.of(collectionId),
            RecordType.valueOf(recordType),
            deleteRecordsRequestServerModel);

    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<EvaluateExpressionsResponseServerModel> evaluateExpressions(
      UUID collectionId,
      String recordType,
      String recordId,
      EvaluateExpressionsRequestServerModel request) {
    permissionService.requireReadPermission(CollectionId.of(collectionId));
    var expressionsMap =
        request.getExpressions().stream()
            .collect(
                Collectors.toMap(
                    NamedExpressionServerModel::getName,
                    NamedExpressionServerModel::getExpression));
    var response =
        expressionService.evaluateExpressions(
            CollectionId.of(collectionId),
            RecordType.valueOf(recordType),
            recordId,
            expressionsMap);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<EvaluateExpressionsWithArrayResponseServerModel>
      evaluateExpressionsWithArray(
          UUID collectionId,
          String recordType,
          String recordId,
          EvaluateExpressionsWithArrayRequestServerModel request) {
    permissionService.requireReadPermission(CollectionId.of(collectionId));
    var expressionsMap =
        request.getExpressions().stream()
            .collect(
                Collectors.toMap(
                    NamedExpressionServerModel::getName,
                    NamedExpressionServerModel::getExpression));
    var response =
        expressionService.evaluateExpressionsWithRelationArray(
            CollectionId.of(collectionId),
            RecordType.valueOf(recordType),
            recordId,
            request.getArrayExpression(),
            expressionsMap,
            request.getPageSize(),
            request.getOffset());
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}
