package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.BackupRestoreDao;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({"mock-storage", "local-cors", "local"})
@ContextConfiguration(name = "mockStorage")
@SpringBootTest
@TestPropertySource(
    properties = {
      "twds.collection.workspace-id=123e4567-e89b-12d3-a456-426614174000",
      "twds.pg_dump.useAzureIdentity=false"
    })
class BackupServiceIntegrationTest {
  @Autowired private BackupRestoreService backupRestoreService;

  @Autowired private BackupRestoreDao<BackupResponse> backupDao;

  @Test
  void testBackupAzureWDS() {
    var trackingId = UUID.randomUUID();
    var sourceWorkspaceId = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
    backupRestoreService.backupAzureWDS(
        "v0.2", trackingId, new BackupRestoreRequest(sourceWorkspaceId, null));

    var response = backupRestoreService.checkBackupStatus(trackingId);
    assertEquals(JobStatus.SUCCEEDED, response.getStatus());

    var backupRecord = backupDao.getStatus(trackingId);
    assertNotNull(backupRecord);
    assertEquals(response.getStatus(), backupRecord.getStatus());
    assertNotNull(backupRecord.getResult().filename());
    assertEquals(backupRecord.getResult().filename(), response.getResult().filename());
  }
}
