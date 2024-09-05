package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.service.PermissionService;
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
import org.springframework.web.bind.annotation.RestController;

@DataPlane
@RestController
public class CloningController {

  private final BackupRestoreService backupRestoreService;
  private final PermissionService permissionService;
  private final TwdsProperties twdsProperties;

  public CloningController(
      BackupRestoreService backupRestoreService,
      PermissionService permissionService,
      TwdsProperties twdsProperties) {
    this.backupRestoreService = backupRestoreService;
    this.permissionService = permissionService;
    this.twdsProperties = twdsProperties;
  }

  @PostMapping("/backup/{version}")
  public ResponseEntity<Job<JobInput, BackupResponse>> createBackup(
      @PathVariable("version") String version,
      @RequestBody BackupRestoreRequest BackupRestoreRequest) {
    permissionService.requireReadPermission(twdsProperties.workspaceId());
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
    permissionService.requireReadPermission(twdsProperties.workspaceId());
    var response = backupRestoreService.checkBackupStatus(trackingId);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @GetMapping("/clone/{version}")
  public ResponseEntity<Job<JobInput, CloneResponse>> getCloningStatus(
      @PathVariable("version") String version) {
    validateVersion(version);
    permissionService.requireReadPermission(twdsProperties.workspaceId());
    var response = backupRestoreService.checkCloneStatus();
    var status = (response == null) ? HttpStatus.NOT_FOUND : HttpStatus.OK;
    return new ResponseEntity<>(response, status);
  }
}
