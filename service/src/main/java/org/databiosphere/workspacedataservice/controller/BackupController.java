package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupService;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

@RestController
public class BackupController {
    private final WorkspaceManagerDao workspaceManagerDao;
    private final BackupService backupService;
    private final AzureBlobStorage storage;

    public BackupController(WorkspaceManagerDao workspaceManagerDao, BackupService backupService) {
        this.workspaceManagerDao = workspaceManagerDao;
        this.storage = new AzureBlobStorage(this.workspaceManagerDao);
        this.backupService = backupService;
    }

    @PostMapping("/backup/{version}")
    public ResponseEntity<BackupResponse> createBackup(@PathVariable("version") String version) {
        BackupResponse response = backupService.backupAzureWDS(storage, version);
        if(response.backupStatus()) {
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}