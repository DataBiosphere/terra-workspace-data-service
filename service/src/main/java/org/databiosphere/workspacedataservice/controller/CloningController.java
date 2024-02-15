package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentRestController.WdsRestController;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@WdsRestController(deploymentModes = {"data-plane"})
public class CloningController {

  private final BackupRestoreService backupRestoreService;

  public CloningController(BackupRestoreService backupRestoreService) {
    this.backupRestoreService = backupRestoreService;
  }

  @PostMapping("/backup/{version}")
  public ResponseEntity<Job<JobInput, BackupResponse>> createBackup(
      @PathVariable("version") String version,
      @RequestBody BackupRestoreRequest BackupRestoreRequest) {
    UUID trackingId = UUID.randomUUID();
    // TODO: make async
    Job<JobInput, BackupResponse> backupJob =
        backupRestoreService.backupAzureWDS(version, trackingId, BackupRestoreRequest);
    return new ResponseEntity<>(backupJob, HttpStatus.OK);
  }

  @GetMapping("/backup/{version}/{trackingId}")
  public ResponseEntity<Job<JobInput, BackupResponse>> getBackupStatus(
      @PathVariable("version") String version, @PathVariable("trackingId") UUID trackingId) {
    validateVersion(version);
    var response = backupRestoreService.checkBackupStatus(trackingId);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @GetMapping("/clone/{version}")
  public ResponseEntity<Job<JobInput, CloneResponse>> getCloningStatus(
      @PathVariable("version") String version) {
    validateVersion(version);
    var response = backupRestoreService.checkCloneStatus();
    var status = (response == null) ? HttpStatus.NOT_FOUND : HttpStatus.OK;
    return new ResponseEntity<>(response, status);
  }
}
