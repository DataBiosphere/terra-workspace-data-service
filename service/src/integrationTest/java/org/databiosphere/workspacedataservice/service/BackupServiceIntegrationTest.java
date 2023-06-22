package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.databiosphere.workspacedataservice.storage.LocalFileStorage;
import org.databiosphere.workspacedataservice.storage.LocalFileStorageConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

@ActiveProfiles(profiles = { "mock-storage"})
@SpringBootTest(properties = "spring.cache.type=NONE", classes = {LocalFileStorage.class, LocalFileStorageConfig.class})
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"})
public class BackupServiceIntegrationTest {
    @Autowired
    private BackupService backupService;

    private BackUpFileStorage storage = new LocalFileStorage();

    @Test
    void testBackupAzureWDS() throws Exception {

        var trackingId = UUID.randomUUID();
        var sourceWorkspaceId = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
        backupService.backupAzureWDS("v0.2", trackingId, sourceWorkspaceId);

        // todo change the assert to check the internal state instead
        //  assertTrue(response.backupStatus(), response.message())
        backupService.checkBackupStatus(trackingId);
    }
}