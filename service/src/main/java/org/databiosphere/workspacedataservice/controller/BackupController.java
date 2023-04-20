package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BackupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
public class BackupController {

    @Autowired
    private BackupService backupService;

    // TODO: We may not need the workspaceId
    @PostMapping("/backup/azure/{instanceId}")
    public void createBackup(@PathVariable("instanceId") UUID instanceId, @RequestParam(name= "workspaceId", required = true) UUID workspaceId) {
        backupService.backupAzureWDS(instanceId, workspaceId);
    }
}