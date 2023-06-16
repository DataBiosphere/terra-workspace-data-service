package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
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
    public ResponseEntity<BackupRestoreResponse> createBackup(@PathVariable("version") String version) {
        BackupRestoreResponse response = backupRestoreService.backupAzureWDS(storage, version);
        if(response.backupRestoreStatus()) {
            return new ResponseEntity<>(response, HttpStatus.OK);
        }
        
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @PostMapping("/restore/{version}")
    public ResponseEntity<BackupRestoreResponse> restoreBackup(@PathVariable("version") String version) {
        BackupRestoreResponse response = backupRestoreService.restoreAzureWDS(storage, version);
        if(response.backupRestoreStatus()) {
            return new ResponseEntity<>(response, HttpStatus.OK);
        }
        
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}