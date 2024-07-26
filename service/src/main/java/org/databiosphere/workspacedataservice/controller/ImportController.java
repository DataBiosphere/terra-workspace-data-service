package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportApi;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@DataPlane
@ControlPlane
@RestController
public class ImportController implements ImportApi {
  private final ImportService importService;
  private final PermissionService permissionService;

  public ImportController(ImportService importService, PermissionService permissionService) {
    this.importService = importService;
    this.permissionService = permissionService;
  }

  @Override
  public ResponseEntity<GenericJobServerModel> importV1(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    permissionService.requireWritePermission(CollectionId.of(instanceUuid));
    GenericJobServerModel importJob = importService.createImport(instanceUuid, importRequest);
    return new ResponseEntity<>(importJob, HttpStatus.ACCEPTED);
  }
}
