package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({"mock-storage", "local"})
@ContextConfiguration(name = "mockStorage")
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000",
      "twds.instance.source-workspace-id=10000000-0000-0000-0000-000000000111",
      "twds.pg_dump.useAzureIdentity=false"
    })
public class RestoreServiceIntegrationTest {
  @Autowired private BackupRestoreService backupRestoreService;

  @Autowired InstanceDao instanceDao;

  @Autowired RecordDao recordDao;

  @Value("${twds.instance.workspace-id:}")
  private String workspaceId;

  @AfterEach
  void tearDown() {
    // clean up any instances left in the db
    List<UUID> allInstances = instanceDao.listInstanceSchemas();
    allInstances.forEach(instanceId -> instanceDao.dropSchema(instanceId));
    // TODO: also drop any orphaned pg schemas that don't have an entry in the sys_wds.instances
    // table.
    // this can happen when restores fail.
  }

  // this test references the file src/integrationTest/resources/backup-test.sql as its backup
  @Test
  void testRestoreAzureWDS() {
    UUID destInstance = UUID.fromString(workspaceId);

    // confirm neither source nor destination instance should exist in our list of schemas to start
    List<UUID> instancesBefore = instanceDao.listInstanceSchemas();
    assertThat(instancesBefore).isEmpty();

    // perform the restore
    var response =
        backupRestoreService.restoreAzureWDS("v0.2", "backup.sql", UUID.randomUUID(), "");
    assertSame(JobStatus.SUCCEEDED, response.getStatus());

    // After restore, we should have one instance, the destination instance.
    // The source instance should not exist in the db.
    List<UUID> expectedInstances = List.of(destInstance);
    List<UUID> actualInstances = instanceDao.listInstanceSchemas();
    assertEquals(expectedInstances, actualInstances);

    // after restore, destination instance should have one table named "thing"
    // this matches the contents of WDS-integrationTest-LocalFileStorage-input.sql
    List<RecordType> expectedTables = List.of(RecordType.valueOf("thing"));
    List<RecordType> actualTables = recordDao.getAllRecordTypes(destInstance);
    assertEquals(expectedTables, actualTables);
  }
}
