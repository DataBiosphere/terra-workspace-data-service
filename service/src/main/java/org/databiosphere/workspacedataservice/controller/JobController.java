package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.JobApi;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
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

    // return job status, 200 if job is completed, 202 if job is still running, and 500 if
    // we can't determine
    HttpStatus responseCode = JobStatus.fromGeneratedModel(job.getStatus()).httpCode();

    return new ResponseEntity<>(job, responseCode);
  }
}
