package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ActiveProfiles({"mock-storage", "local"})
@ContextConfiguration(name = "mockStorage")
@SpringBootTest
@TestPropertySource(properties = {"twds.pg_dump.useAzureIdentity=false", "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000", "twds.instance.source-workspace-id=123e4567-e89b-12d3-a456-426614174000", "twds.pg_dump.host="})
public class BackupRestoreServiceFailureIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Value("${twds.instance.workspace-id}")
    private String workspaceId;

    @Value("${twds.instance.source-workspace-id}")
    private String sourceWorkspaceId;

    @Test
    void testRestoreAzureWDSErrorHandling() {
        var response = backupRestoreService.restoreAzureWDS("v0.2");
        assertFalse(response);
    }

    @Test
    void testBackupAzureWDS() {
        var trackingId = UUID.randomUUID();
        backupRestoreService.backupAzureWDS("v0.2", trackingId, new BackupRequest(UUID.fromString(sourceWorkspaceId), null));
        var response = backupRestoreService.checkBackupStatus(trackingId);
        assertEquals(true, response.backupStatus());
        assertEquals(BackupSchema.BackupState.ERROR.toString(), response.state());
    }
}