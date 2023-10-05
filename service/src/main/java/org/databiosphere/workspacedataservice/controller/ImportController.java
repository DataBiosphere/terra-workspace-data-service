package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportApi;
import org.databiosphere.workspacedataservice.generated.ImportJobStatusServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
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
  public ResponseEntity<ImportJobStatusServerModel> importV1(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    // TODO: validate instance
    // TODO: validate user has write permission on instance
    // TODO: validate importRequest, e.g. does it contain a valid URL

    // save this import request to the WDS db and queue up its async execution in Quartz
    ImportStatusResponse jobStatus = importService.queueJob(importRequest);

    return new ResponseEntity<>(jobStatus, HttpStatus.ACCEPTED);
  }

  @Override
  public ResponseEntity<ImportJobStatusServerModel> importStatusV1(
      UUID instanceUuid, String jobId) {
    // TODO: validate instance (this only requires read permission, no permission checks required)
    // TODO: validate jobId is non-empty

    // retrieve jobId from the job store
    ImportStatusResponse importJob = importService.getJob(jobId);

    // return job status, 200 if job is completed and 202 if job is still running
    HttpStatus responseCode = HttpStatus.ACCEPTED;
    if (importJob.getStatus() == ImportJobStatusServerModel.StatusEnum.SUCCEEDED
        || importJob.getStatus() == ImportJobStatusServerModel.StatusEnum.ERROR) {
      responseCode = HttpStatus.OK;
    }

    return new ResponseEntity<>(importJob, responseCode);
  }
}
