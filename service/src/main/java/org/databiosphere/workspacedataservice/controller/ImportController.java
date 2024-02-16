package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportApi;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ImportController implements ImportApi {
  private final ImportService importService;

  public ImportController(ImportService importService) {
    this.importService = importService;
  }

  @Override
  public ResponseEntity<GenericJobServerModel> importV1(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    GenericJobServerModel importJob = importService.createImport(instanceUuid, importRequest);
    return new ResponseEntity<>(importJob, HttpStatus.ACCEPTED);
  }

  @Override
  public ResponseEntity<GenericJobServerModel> importV2(
      UUID collectionUuid, ImportRequestServerModel importRequest) {
    GenericJobServerModel importJob = importService.createImport(collectionUuid, importRequest);
    return new ResponseEntity<>(importJob, HttpStatus.ACCEPTED);
  }
}
