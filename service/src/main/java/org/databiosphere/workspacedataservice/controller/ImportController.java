package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportApi;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@DataPlane
@ControlPlane
@RestController
public class ImportController implements ImportApi {
  private final ImportService importService;

  public ImportController(ImportService importService) {
    this.importService = importService;
  }

  @Override
  public ResponseEntity<GenericJobServerModel> importV1(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    GenericJobServerModel importJob =
        importService.createImport(CollectionId.of(instanceUuid), importRequest);
    return new ResponseEntity<>(importJob, HttpStatus.ACCEPTED);
  }
}
