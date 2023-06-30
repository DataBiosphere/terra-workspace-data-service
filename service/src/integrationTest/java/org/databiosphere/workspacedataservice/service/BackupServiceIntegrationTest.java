package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ActiveProfiles({"mock-storage", "local"})
@ContextConfiguration(name = "mockStorage")
@SpringBootTest
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000", "twds.pg_dump.useAzureIdentity=false"})
class BackupServiceIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Autowired
    private BackupDao backupDao;
    @Test
    void testBackupAzureWDS() {
        var trackingId = UUID.randomUUID();
        var sourceWorkspaceId = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
        backupRestoreService.backupAzureWDS("v0.2", trackingId, new BackupRequest(sourceWorkspaceId, null));

        var response = backupRestoreService.checkBackupStatus(trackingId);
        assertEquals(true, response.backupStatus());
        assertEquals(BackupSchema.BackupState.COMPLETED.toString(), response.state());

        var backupRecord = backupDao.getBackupStatus(trackingId);
        assertNotNull(backupRecord);
        assertEquals(response.state(), backupRecord.getState().toString());
        assertNotNull(backupRecord.getFilename());
        assertEquals(backupRecord.getFilename(), response.filename());
    }
}