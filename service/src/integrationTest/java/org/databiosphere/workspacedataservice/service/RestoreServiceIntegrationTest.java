package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.IntegrationServiceTestBase;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.startup.CollectionInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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
      "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000",
      "twds.instance.source-workspace-id=10000000-0000-0000-0000-000000000111",
      "twds.pg_dump.useAzureIdentity=false"
    })
class RestoreServiceIntegrationTest extends IntegrationServiceTestBase {

  @Autowired private BackupRestoreService backupRestoreService;
  @Autowired CollectionDao collectionDao;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired RecordDao recordDao;
  @Autowired @SingleTenant WorkspaceId workspaceId;

  // Don't run the CollectionInitializer on startup, so this test can start with a clean slate.
  // By making an (empty) mock bean to replace CollectionInitializer, we ensure it is a noop.
  @MockBean CollectionInitializer mockCollectionInitializer;

  // ensure we clean up the db after our tests
  @AfterEach
  void cleanUp() {
    cleanDb(collectionDao, namedTemplate);
  }

  // this test references the file src/integrationTest/resources/backup-test.sql as its backup
  @Test
  void testRestoreAzureWDS() {
    CollectionId destCollection = CollectionId.of(workspaceId.id());

    // confirm neither source nor destination collection should exist in our list of schemas to
    // start
    List<CollectionId> collectionsBefore = collectionDao.listCollectionSchemas();
    assertThat(collectionsBefore).isEmpty();

    // perform the restore
    var response =
        backupRestoreService.restoreAzureWDS("v0.2", "backup.sql", UUID.randomUUID(), "");
    assertSame(JobStatus.SUCCEEDED, response.getStatus());

    // After restore, we should have one collection, the destination collection.
    // The source collection should not exist in the db.
    List<CollectionId> expectedCollections = List.of(destCollection);
    List<CollectionId> actualCollections = collectionDao.listCollectionSchemas();
    assertEquals(expectedCollections, actualCollections);

    // after restore, destination collection should have one table named "thing"
    // this matches the contents of WDS-integrationTest-LocalFileStorage-input.sql
    List<RecordType> expectedTables = List.of(RecordType.valueOf("thing"));
    List<RecordType> actualTables = recordDao.getAllRecordTypes(destCollection.id());
    assertEquals(expectedTables, actualTables);
  }
}
