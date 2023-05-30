package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.BackupService;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class BackupController {

    private final BackupService backupService;
    private final AzureBlobStorage storage;

    public BackupController(BackupService backupService) {
        this.storage = new AzureBlobStorage();
        this.backupService = backupService;
    }

    @PostMapping("/backup")
    @RetryableApi
    public ResponseEntity<String> createBackup() throws Exception {
        if(backupService.backupAzureWDS(storage)) {
            return new ResponseEntity<>(HttpStatus.OK);
        }
        
        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}