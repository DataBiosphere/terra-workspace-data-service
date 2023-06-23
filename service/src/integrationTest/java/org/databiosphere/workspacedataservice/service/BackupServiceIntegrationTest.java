package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.BackupDao;
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
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"})
class BackupServiceIntegrationTest {
    @Autowired
    private BackupService backupService;

    @Autowired
    private BackupDao backupDao;
    @Test
    void testBackupAzureWDS() throws Exception {
        var trackingId = UUID.randomUUID();
        var sourceWorkspaceId = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
        backupService.backupAzureWDS("v0.2", trackingId, sourceWorkspaceId);

        var response = backupService.checkBackupStatus(trackingId);
        assertEquals(true, response.backupStatus());
        assertEquals("COMPLETED", response.state());

        var backupRecord = backupDao.getBackupStatus(trackingId);
        assertNotNull(backupRecord);
        assertEquals(response.state(), backupRecord.getState().toString());
        assertNotNull(backupRecord.getFilename());
        assertEquals(backupRecord.getFilename(), response.filename());
    }
}