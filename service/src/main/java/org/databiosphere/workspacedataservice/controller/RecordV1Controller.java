package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.DeleteRecordsRequestServerModel;
import org.databiosphere.workspacedataservice.generated.DeleteRecordsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.RecordApi;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@ConditionalOnProperty(name = "controlPlanePreview", havingValue = "on")
@RestController
public class RecordV1Controller implements RecordApi {

  private final RecordOrchestratorService recordOrchestratorService;
  private final PermissionService permissionService;

  public RecordV1Controller(
      RecordOrchestratorService recordOrchestratorService, PermissionService permissionService) {
    this.recordOrchestratorService = recordOrchestratorService;
    this.permissionService = permissionService;
  }

  @Override
  public ResponseEntity<DeleteRecordsResponseServerModel> deleteRecords(
      UUID collectionId,
      String recordType,
      DeleteRecordsRequestServerModel deleteRecordsRequestServerModel) {
    permissionService.requireWritePermission(CollectionId.of(collectionId));
    return recordOrchestratorService.deleteRecords(
        CollectionId.of(collectionId),
        RecordType.valueOf(recordType),
        deleteRecordsRequestServerModel);
  }
}
