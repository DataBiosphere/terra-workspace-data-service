package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupTrackingResponse;
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
    public ResponseEntity<BackupTrackingResponse> createBackup(@PathVariable("version") String version,
                                                               @RequestBody BackupRequest backupRequest) {
        UUID trackingId = UUID.randomUUID();
        // TODO: make async
        backupRestoreService.backupAzureWDS(version, trackingId, backupRequest);
        return new ResponseEntity<>(new BackupTrackingResponse(String.valueOf(trackingId)), HttpStatus.OK);
    }

    @GetMapping("/backup/{version}/{trackingId}")
    public ResponseEntity<BackupResponse> getBackupStatus(@PathVariable("version") String version, @PathVariable("trackingId") UUID trackingId) {
        validateVersion(version);
        var response = backupRestoreService.checkBackupStatus(trackingId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}