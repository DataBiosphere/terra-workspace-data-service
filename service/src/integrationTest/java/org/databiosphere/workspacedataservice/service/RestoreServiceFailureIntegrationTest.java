package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertFalse;

@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000", "twds.instance.source-workspace-id=123e4567-e89b-12d3-a456-426614174000"})
public class RestoreServiceFailureIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    private LocalFileStorage storage = new LocalFileStorage();

    @Test
    void testRestoreAzureWDSErrorHandling() {
        BackupRestoreResponse response = backupRestoreService.backupAzureWDS(storage, "v0.2");
        assertFalse(response.backupRestoreStatus());
    }
}