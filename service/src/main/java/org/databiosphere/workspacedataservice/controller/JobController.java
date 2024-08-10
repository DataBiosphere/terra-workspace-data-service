package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.JobApi;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Controller for job-related APIs */
@DataPlane
@ControlPlane
@RestController
public class JobController implements JobApi {

  JobService jobService;
  private final PermissionService permissionService;

  public JobController(JobService jobService, PermissionService permissionService) {
    this.jobService = jobService;
    this.permissionService = permissionService;
  }

  @Override
  public ResponseEntity<GenericJobServerModel> jobStatusV1(UUID jobId) {
    GenericJobServerModel job = jobService.getJob(jobId);
    permissionService.requireReadPermission(CollectionId.of(job.getInstanceId()));

    // return job status, 200 if job is completed, 202 if job is still running, and 500 if
    // we can't determine
    HttpStatus responseCode = JobStatus.fromGeneratedModel(job.getStatus()).httpCode();

    return new ResponseEntity<>(job, responseCode);
  }

  @Override
  public ResponseEntity<List<GenericJobServerModel>> jobsInInstanceV1(
      UUID instanceUuid, @Nullable List<String> statuses) {
    // status is an optional parameter
    if (statuses != null) {
      try {
        // validate the strings in status are all valid statuses before proceeding
        for (var statusValue : statuses) {
          StatusEnum.fromValue(statusValue);
        }
      } catch (IllegalArgumentException e) {
        throw new ResponseStatusException(
            HttpStatus.BAD_REQUEST, "Invalid status type provided.", e);
      }
    }
    permissionService.requireReadPermission(CollectionId.of(instanceUuid));
    List<GenericJobServerModel> jobList =
        jobService.getJobsForCollection(
            CollectionId.of(instanceUuid),
            statuses == null || statuses.isEmpty() ? Optional.empty() : Optional.of(statuses));

    return new ResponseEntity<>(jobList, HttpStatus.OK);
  }
}
