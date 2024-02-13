package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.JobApi;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
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
  public ResponseEntity<GenericJobServerModel> jobStatusV1(UUID jobId) {
    GenericJobServerModel job = jobService.getJob(jobId);

    // return job status, 200 if job is completed, 202 if job is still running, and 500 if
    // we can't determine
    HttpStatus responseCode = JobStatus.fromGeneratedModel(job.getStatus()).httpCode();

    return new ResponseEntity<>(job, responseCode);
  }

  @Override
  public ResponseEntity<List<GenericJobServerModel>> jobsInInstanceV1(
      UUID instanceUuid, List<String> statuses) {
    // status is an optional parameter
    if (statuses != null) {
      try {
        // validate the strings in status are all valid statuses before proceeding
        for (var statusValue : statuses) {
          StatusEnum.fromValue(statusValue);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid status type provided.", e);
      }
    }
    List<GenericJobServerModel> jobList =
        jobService.getJobsForInstance(CollectionId.of(instanceUuid), statuses);

    return new ResponseEntity<>(jobList, HttpStatus.OK);
  }
}
