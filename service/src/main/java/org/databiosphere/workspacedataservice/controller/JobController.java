package org.databiosphere.workspacedataservice.controller;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.JobApi;
import org.databiosphere.workspacedataservice.model.InstanceId;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

/** Controller for job-related APIs */
@RestController
public class JobController implements JobApi {

  InstanceService instanceService;
  JobService jobService;

  PermissionService permissionService;

  public JobController(
      InstanceService instanceService, JobService jobService, PermissionService permissionService) {
    this.instanceService = instanceService;
    this.jobService = jobService;
    this.permissionService = permissionService;
  }

  @Override
  public ResponseEntity<GenericJobServerModel> jobStatusV2(UUID instanceUuid, UUID jobId) {
    InstanceId instanceId = new InstanceId(instanceUuid);
    // TODO davidan: move permission checks to the service layer, not in the controller?
    // check permission on the instance; under the covers this resolves the workspace
    // associated with the instance and checks permission on that workspace
    BearerToken bearerToken = TokenContextUtil.getToken();
    boolean hasPermission = permissionService.canReadInstance(instanceId, bearerToken.getValue());
    if (!hasPermission) {
      throw new RuntimeException("disallowed!");
    }
    // retrieve the job, including a check on if this job executed in this instance
    GenericJobServerModel job = jobService.getJob(instanceId, jobId);
    // generate an appropriate response
    if (job == null) {
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
    HttpStatus responseCode = JobStatus.fromGeneratedModel(job.getStatus()).httpCode();
    return new ResponseEntity<>(job, responseCode);
  }

  @Override
  public ResponseEntity<List<GenericJobServerModel>> listJobsV2(UUID instanceUuid) {
    InstanceId instanceId = new InstanceId(instanceUuid);
    // TODO davidan: move permission checks to the service layer, not in the controller?
    // check permission on the instance; under the covers this resolves the workspace
    // associated with the instance and checks permission on that workspace
    BearerToken bearerToken = TokenContextUtil.getToken();
    boolean hasPermission = permissionService.canReadInstance(instanceId, bearerToken.getValue());
    if (!hasPermission) {
      throw new RuntimeException("disallowed!");
    }
    List<GenericJobServerModel> jobs = jobService.listJobs(instanceId);
    return new ResponseEntity<>(jobs, HttpStatus.OK);
  }

  @Override
  @Deprecated
  public ResponseEntity<GenericJobServerModel> jobStatusV1(UUID jobId) {
    GenericJobServerModel job = jobService.getJob(jobId);

    // return job status, 200 if job is completed, 202 if job is still running, and 500 if
    // we can't determine
    HttpStatus responseCode = JobStatus.fromGeneratedModel(job.getStatus()).httpCode();

    return new ResponseEntity<>(job, responseCode);
  }
}
