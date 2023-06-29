package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class CloningController {

    private final BackupRestoreService backupRestoreService;
    private final AzureBlobStorage storage;
    public CloningController(BackupRestoreService backupRestoreService) {
        this.storage = new AzureBlobStorage();
        this.backupRestoreService = backupRestoreService;
    }

    @PostMapping("/backup/{version}")
    public ResponseEntity<BackupResponse> createBackup(@PathVariable("version") String version) {
        BackupResponse response = backupRestoreService.backupAzureWDS(storage, version);
        var status = response.backupStatus() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR;
        return new ResponseEntity<>(response, status);
    }
}