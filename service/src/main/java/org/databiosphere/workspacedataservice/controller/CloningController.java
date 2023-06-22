package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupService;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupTrackingResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class CloningController {
    private final BackupService backupService;
    public CloningController(BackupService backupService) {
        this.backupService = backupService;
    }

    @PostMapping("/backup/{version}/{requestorWorkspaceId}")
    public ResponseEntity<BackupTrackingResponse> createBackup(@PathVariable("version") String version, @PathVariable("requestorWorkspaceId") UUID requestorWorkspaceId) {
        UUID trackingId = UUID.randomUUID();
        // TODO: make async
        backupService.backupAzureWDS(version, trackingId, requestorWorkspaceId);
        return new ResponseEntity<>(new BackupTrackingResponse(String.valueOf(trackingId)), HttpStatus.OK);
    }

    @PostMapping("/backup/status/{trackingId}")
    public ResponseEntity<BackupResponse> getBackupStatus(@PathVariable("trackingId") UUID trackingId) {
        var response = backupService.checkBackupStatus(trackingId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}