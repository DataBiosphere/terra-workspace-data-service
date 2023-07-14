package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.BackupRestoreDao;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
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
@TestPropertySource(properties = {
        "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000",
        "twds.pg_dump.useAzureIdentity=false"
})
class BackupServiceIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Autowired
    private BackupRestoreDao BackupRestoreDao;
    @Test
    void testBackupAzureWDS() {
        var trackingId = UUID.randomUUID();
        var sourceWorkspaceId = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
        backupRestoreService.backupAzureWDS("v0.2", trackingId, new BackupRestoreRequest(sourceWorkspaceId, null));

        var response = backupRestoreService.checkStatus(trackingId, true);
        assertEquals(JobStatus.SUCCEEDED, response.getStatus());

        var backupRecord = BackupRestoreDao.getStatus(trackingId, true);
        assertNotNull(backupRecord);
        assertEquals(response.getStatus(), backupRecord.getStatus());
        assertNotNull(backupRecord.getResult().filename());
        assertEquals(backupRecord.getResult().filename(), response.getResult().filename());
    }
}