package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.JobApi;
import org.databiosphere.workspacedataservice.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

/** Controller for job-related APIs */
@RestController
public class JobController implements JobApi {

  JobService jobService;

  public JobController(JobService jobService) {
    this.jobService = jobService;
  }

  @Override
  public ResponseEntity<GenericJobServerModel> jobStatusV1(UUID instanceUuid, UUID jobId) {
    GenericJobServerModel job = jobService.getJob(instanceUuid, jobId);

    // return job status, 200 if job is completed and 202 if job is still running
    HttpStatus responseCode = HttpStatus.ACCEPTED;
    if (job.getStatus() == GenericJobServerModel.StatusEnum.SUCCEEDED
        || job.getStatus() == GenericJobServerModel.StatusEnum.ERROR) {
      responseCode = HttpStatus.OK;
    }

    return new ResponseEntity<>(job, responseCode);
  }
}
