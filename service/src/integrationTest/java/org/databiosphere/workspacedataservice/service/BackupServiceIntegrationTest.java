package org.databiosphere.workspacedataservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

@ActiveProfiles({"mock-storage"})
@SpringBootTest(properties = "spring.cache.type=NONE")//, classes = {LocalFileStorage.class, LocalFileStorageConfig.class})
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"})
public class BackupServiceIntegrationTest {
    @Autowired
    private BackupService backupService;

    @Test
    void testBackupAzureWDS() throws Exception {
        var staus = backupService.checkBackupStatus(UUID.fromString("08c8326f-d80f-4d85-92d8-1bc3929dbf6f"));
        var trackingId = UUID.randomUUID();
        var sourceWorkspaceId = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
        backupService.backupAzureWDS("v0.2", trackingId, sourceWorkspaceId);

        // todo change the assert to check the internal state instead
        //  assertTrue(response.backupStatus(), response.message())
        backupService.checkBackupStatus(trackingId);
    }
}