package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.UUID;
import org.databiosphere.workspacedataservice.IntegrationServiceTestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({"mock-storage", "local-cors", "local", "data-plane"})
@ContextConfiguration(name = "mockStorage")
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      "twds.pg_dump.useAzureIdentity=false",
      "twds.instance.source-workspace-id=123e4567-e89b-12d3-a456-426614174001",
      "twds.pg_dump.host="
    })
class BackupRestoreServiceFailureIntegrationTest extends IntegrationServiceTestBase {
  @Autowired private BackupRestoreService backupRestoreService;
  @Autowired CollectionDao collectionDao;
  @Autowired NamedParameterJdbcTemplate namedTemplate;

  @Value("${twds.instance.source-workspace-id}")
  private String sourceWorkspaceId;

  // ensure we clean up the db after our tests
  @AfterEach
  void cleanUp() {
    cleanDb(collectionDao, namedTemplate);
  }

  @Test
  void testRestoreAzureWDSErrorHandling() {
    Job<JobInput, RestoreResponse> response =
        backupRestoreService.restoreAzureWDS("v0.2", "backup.sql", UUID.randomUUID(), "");
    // will fail because twds.pg_dump.host is blank
    assertSame(JobStatus.ERROR, response.getStatus());
    assertFalse(response.getErrorMessage().isBlank());
  }

  @Test
  void testBackupAzureWDS() {
    var trackingId = UUID.randomUUID();
    backupRestoreService.backupAzureWDS(
        "v0.2", trackingId, new BackupRestoreRequest(UUID.fromString(sourceWorkspaceId), null));
    var response = backupRestoreService.checkBackupStatus(trackingId);
    // will fail because twds.pg_dump.host is blank
    assertEquals(JobStatus.ERROR, response.getStatus());
  }
}
