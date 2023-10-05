package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.JobApi;
import org.databiosphere.workspacedataservice.generated.JobStatusServerModel;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

/** Controller for import-related APIs */
@RestController
public class JobController implements JobApi {

  private final ImportService importService;

  public JobController(ImportService importService) {
    this.importService = importService;
  }

  @Override
  public ResponseEntity<JobStatusServerModel> jobStatusV1(UUID instanceUuid, UUID jobId) {
    // TODO: validate instance (this only requires read permission, no permission checks required)
    // TODO: validate jobId is non-empty

    // retrieve jobId from the job store
    ImportStatusResponse importJob = importService.getJob(jobId);

    // return job status, 200 if job is completed and 202 if job is still running
    HttpStatus responseCode = HttpStatus.ACCEPTED;
    if (importJob.getStatus() == JobStatusServerModel.StatusEnum.SUCCEEDED
        || importJob.getStatus() == JobStatusServerModel.StatusEnum.ERROR) {
      responseCode = HttpStatus.OK;
    }

    return new ResponseEntity<>(importJob, responseCode);
  }
}
