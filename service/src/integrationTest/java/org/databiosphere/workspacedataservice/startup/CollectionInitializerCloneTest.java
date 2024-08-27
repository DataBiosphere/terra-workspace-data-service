package org.databiosphere.workspacedataservice.startup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedata.api.CloningApi;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.BackupJob;
import org.databiosphere.workspacedata.model.BackupResponse;
import org.databiosphere.workspacedataservice.IntegrationServiceTestBase;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

// "local" profile prevents CollectionInitializerBean from running at Spring startup;
// that way, we can run it when we want to inside our tests.
@ActiveProfiles({"mock-storage", "local-cors", "local", "data-plane"})
@TestPropertySource(
    properties = {
      // source id must match value in WDS-integrationTest-LocalFileStorage-input.sql
      "twds.instance.source-workspace-id=10000000-0000-0000-0000-000000000111",
      "twds.pg_dump.useAzureIdentity=false"
    })
@DirtiesContext
@SpringBootTest
class CollectionInitializerCloneTest extends IntegrationServiceTestBase {

  // standard beans
  @Autowired CollectionInitializerBean collectionInitializerBean;
  @Autowired CollectionDao collectionDao;
  @Autowired CollectionService collectionService;
  @Autowired RecordDao recordDao;
  @Autowired CloneDao cloneDao;
  @Autowired NamedParameterJdbcTemplate namedTemplate;

  // mock beans
  @MockBean WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;
  @MockBean LeonardoDao mockLeonardoDao;
  @MockBean WorkspaceManagerDao workspaceManagerDao;

  // Don't run the CollectionInitializer on startup, so this test can start with a clean slate.
  // By making an (empty) mock bean to replace CollectionInitializer, we ensure it is a noop.
  @MockBean CollectionInitializer mockCollectionInitializer;

  // values
  @Autowired @SingleTenant WorkspaceId workspaceId;

  @Value("${twds.instance.source-workspace-id}")
  String sourceWorkspaceId;

  // ensure we clean up the db after our tests
  @AfterEach
  void cleanUp() {
    cleanDb(collectionService, namedTemplate);
  }

  /*
   * If the remote source workspace returns a 404 from /backup/{v},
   * we assume the source workspace is too old and can't be cloned.
   * Test that we return a good error message in this case and that
   * the clone job is in ERROR/BACKUPERROR status.
   */
  @Test
  void remoteWdsDoesntSupportBackup() throws ApiException {
    // set up mocks:
    // leonardo dao returns a fake url; the url doesn't matter for this test
    given(mockLeonardoDao.getWdsEndpointUrl(any())).willReturn("https://unit.test:7777");
    // source workspace returns 404 for the /backup/{v} API
    CloningApi mockCloningApi = Mockito.mock(CloningApi.class);
    given(mockCloningApi.createBackup(any(), any()))
        .willThrow(new ApiException(404, "Not Found for unit test"));
    // and the wdsClientFactory uses the mock cloning API
    given(workspaceDataServiceClientFactory.getBackupClient(any(), any()))
        .willReturn(mockCloningApi);

    // attempt to clone
    collectionInitializerBean.initializeCollection();

    // clone job should have errored, with friendly error message
    Job<JobInput, CloneResponse> cloneStatus = cloneDao.getCloneStatus();
    assertSame(JobStatus.ERROR, cloneStatus.getStatus());
    assertSame(CloneStatus.BACKUPERROR, cloneStatus.getResult().status());
    assertEquals(
        "The data tables in the workspace being cloned do not support cloning. "
            + "Contact the workspace owner to upgrade the version of data tables in that workspace.",
        cloneStatus.getErrorMessage());

    // default collection should exist, with no tables in it
    UUID workspaceUuid = workspaceId.id();
    assertTrue(collectionService.exists(CollectionId.of(workspaceUuid)));
    assertThat(recordDao.getAllRecordTypes(workspaceUuid)).isEmpty();
  }

  /*
   * Test a successful clone operation, using Mockito mocks for Leo, WSM, and WDS clients,
   * plus our custom mocks for blob storage and Sam.
   */
  @Test
  void cloneSuccess() throws ApiException {
    // set up mocks:
    // leonardo dao returns a fake url; the url doesn't matter for this test
    given(mockLeonardoDao.getWdsEndpointUrl(any())).willReturn("https://unit.test:7777");
    // workspace manager dao returns a fake SAS token; it doesn't matter for this test
    given(workspaceManagerDao.getBlobStorageUrl(any(), any()))
        .willReturn("https://sas.fake.unit.test:8888/");
    // source workspace returns a successful BackupJob from /backup/{v}
    BackupResponse sourceBackupResponse = new BackupResponse();
    sourceBackupResponse.setFilename("/fake/filename/for/unit/test");
    BackupJob sourceBackupJob = new BackupJob();
    sourceBackupJob.setStatus(BackupJob.StatusEnum.SUCCEEDED);
    sourceBackupJob.setResult(sourceBackupResponse);

    CloningApi mockCloningApi = Mockito.mock(CloningApi.class);
    given(mockCloningApi.createBackup(any(), any())).willReturn(sourceBackupJob);
    // and the wdsClientFactory uses the mock cloning API
    given(workspaceDataServiceClientFactory.getBackupClient(any(), any()))
        .willReturn(mockCloningApi);

    // attempt to clone
    collectionInitializerBean.initializeCollection();

    // clone job should have succeeded
    Job<JobInput, CloneResponse> cloneStatus = cloneDao.getCloneStatus();
    assertSame(JobStatus.SUCCEEDED, cloneStatus.getStatus());
    assertSame(CloneStatus.RESTORESUCCEEDED, cloneStatus.getResult().status());

    // default collection should exist, with a single table named "thing" in it
    // the "thing" table is defined in WDS-integrationTest-LocalFileStorage-input.sql.
    UUID workspaceUuid = workspaceId.id();
    List<CollectionId> actualCollections =
        collectionService.list(workspaceId).stream()
            .map(coll -> CollectionId.of(coll.getId()))
            .toList();
    assertEquals(List.of(CollectionId.of(workspaceUuid)), actualCollections);
    List<RecordType> actualTypes = recordDao.getAllRecordTypes(workspaceUuid);
    assertEquals(List.of(RecordType.valueOf("thing")), actualTypes);

    // the restored collection should be associated with the current workspace, and should
    // have its name and description populated correctly
    Map<String, Object> rowMap =
        namedTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.collection where id = :id",
            new MapSqlParameterSource("id", workspaceUuid));

    assertEquals(workspaceUuid, rowMap.get("id"));
    assertEquals(workspaceUuid, rowMap.get("workspace_id"));
    assertEquals("default", rowMap.get("name"));
    assertEquals("default", rowMap.get("description"));
  }

  // TODO: if a clone entry already exists, initializeCollection won't do anything
  // TODO: test if backup succeeds but restore fails
  // TODO: what other coverage?

}
