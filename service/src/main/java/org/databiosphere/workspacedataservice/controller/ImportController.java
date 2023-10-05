package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportApi;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.JobStatusServerModel;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

/** Controller for import-related APIs */
@RestController
public class ImportController implements ImportApi {

  private final ImportService importService;

  public ImportController(ImportService importService) {
    this.importService = importService;
  }

  @Override
  public ResponseEntity<JobStatusServerModel> importV1(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    // TODO: validate instance
    // TODO: validate user has write permission on instance
    // TODO: validate importRequest, e.g. does it contain a valid URL

    // save this import request to the WDS db and queue up its async execution in Quartz
    ImportStatusResponse jobStatus = importService.queueJob(importRequest);

    return new ResponseEntity<>(jobStatus, HttpStatus.ACCEPTED);
  }
}
