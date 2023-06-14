package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.service.BackupService;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupTrackingResponse;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class BackupController {
    private final WorkspaceManagerDao workspaceManagerDao;
    private final BackupDao backupDao;
    private final BackupService backupService;
    private final AzureBlobStorage storage;

    public BackupController(WorkspaceManagerDao workspaceManagerDao, BackupDao backupDao, BackupService backupService) {
        this.workspaceManagerDao = workspaceManagerDao;
        this.backupDao = backupDao;
        this.storage = new AzureBlobStorage(this.workspaceManagerDao);
        this.backupService = backupService;
    }

    @PostMapping("/backup/{version}")
    public ResponseEntity<BackupTrackingResponse> createBackup(@PathVariable("version") String version, String requestorWorkspaceId) {
        UUID trackingId = UUID.randomUUID();
        // need to read on how to make this async and keep executing in the background after the controller has returned (and that session is no longer active)
        // need to verify that the token gets taken from api call and doesn't need to passed explicitly when source receives this
        backupDao.createBackupEntry(trackingId, UUID.fromString(requestorWorkspaceId));
        backupService.backupAzureWDS(storage, version, trackingId);
        return new ResponseEntity<>(new BackupTrackingResponse(String.valueOf(trackingId)), HttpStatus.OK);
    }

    @PostMapping("/backup/status/{trackingId}")
    public ResponseEntity<BackupResponse> getBackupStatus(@PathVariable("trackingId") UUID trackingId) {
        var response = backupService.checkBackupStatus(trackingId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}