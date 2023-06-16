package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupService;
import org.databiosphere.workspacedataservice.service.RestoreService;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class CloningController {

    private final BackupService backupService;
    private final RestoreService restoreService;
    private final AzureBlobStorage storage;
    public CloningController(BackupService backupService, RestoreService restoreService) {
        this.storage = new AzureBlobStorage();
        this.backupService = backupService;
        this.restoreService = restoreService;
    }

    @PostMapping("/backup/{version}")
    public ResponseEntity<BackupResponse> createBackup(@PathVariable("version") String version) {
        BackupResponse response = backupService.backupAzureWDS(storage, version);
        if(response.backupStatus()) {
            return new ResponseEntity<>(response, HttpStatus.OK);
        }
        
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @PostMapping("/restore/{version}")
    public ResponseEntity<RestoreResponse> restoreBackup(@PathVariable("version") String version) {
        RestoreResponse response = restoreService.restoreAzureWDS(storage, version);
        if(response.restoreStatus()) {
            return new ResponseEntity<>(response, HttpStatus.OK);
        }
        
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}