package org.databiosphere.workspacedataservice.controller;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.ImportApi;
import org.databiosphere.workspacedataservice.generated.ImportJobStatusServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ImportController implements ImportApi {

  @Override
  public ResponseEntity<ImportJobStatusServerModel> importV1(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    // TODO: validate instance
    // TODO: validate user has write permission on instance
    // TODO: validate importRequest, e.g. does it contain a valid URL
    // TODO: generate jobId
    // TODO: persist import job to Quartz
    // TODO: return jobId to caller, noting the job is Accepted
    return ImportApi.super.importV1(instanceUuid, importRequest);
  }

  @Override
  public ResponseEntity<ImportJobStatusServerModel> importStatusV1(
      UUID instanceUuid, String jobId) {
    // TODO: validate instance (this only requires read permission, no permission checks required)
    // TODO: validate jobId is non-empty
    // TODO: retrieve jobId from the job store
    // TODO: return job status, 200 if job is completed and 202 if job is still running
    return ImportApi.super.importStatusV1(instanceUuid, jobId);
  }
}
