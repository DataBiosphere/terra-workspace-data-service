package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;


@SpringBootTest(properties = "spring.cache.type=NONE")
public class RestoreServiceIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    private LocalFileStorage storage = new LocalFileStorage();
    
    @Test
    void testRestoreAzureWDS() throws Exception {
        // Create the local pg_dump file by calling backup first.
        backupRestoreService.backupAzureWDS(storage, "v0.2");
        var response = backupRestoreService.restoreAzureWDS(storage, "v0.2");
        assertTrue(response.backupRestoreStatus(), response.message());
    }

    @Test
    void testRestoreAzureWDSErrorHandling() {
        BackupRestoreResponse response = backupRestoreService.backupAzureWDS(null, "v0.2");
        assertFalse(response.backupRestoreStatus());
    }
}
