package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@RestController
public class CloningController {

    private final BackupRestoreService backupRestoreService;
    public CloningController(BackupRestoreService backupRestoreService) {
        this.backupRestoreService = backupRestoreService;
    }

    @PostMapping("/backup/{version}")
    public ResponseEntity<Job<BackupRestoreResponse>> createBackup(@PathVariable("version") String version,
                                                               @RequestBody BackupRestoreRequest BackupRestoreRequest) {
        UUID trackingId = UUID.randomUUID();
        // TODO: make async
        Job<BackupRestoreResponse> backupJob = backupRestoreService.backupAzureWDS(version, trackingId, BackupRestoreRequest);
        return new ResponseEntity<>(backupJob, HttpStatus.OK);
    }

    @GetMapping("/backup/{version}/{trackingId}")
    public ResponseEntity<Job<BackupRestoreResponse>> getBackupStatus(@PathVariable("version") String version, @PathVariable("trackingId") UUID trackingId) {
        validateVersion(version);
        var response = backupRestoreService.checkStatus(trackingId, true);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/clone/{version}")
    public ResponseEntity<Job<CloneResponse>> getCloningStatus(@PathVariable("version") String version) {
        validateVersion(version);
        var response = backupRestoreService.checkCloneStatus();
        var status = (response == null) ? HttpStatus.NOT_FOUND : HttpStatus.OK;
        return new ResponseEntity<>(response, status);
    }
}