package org.databiosphere.workspacedataservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedata.api.CloningApi;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.BackupJob;
import org.databiosphere.workspacedata.model.BackupResponse;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
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
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

// "local" profile prevents InstanceInitializerBean from running at Spring startup;
// that way, we can run it when we want to inside our tests.
@ActiveProfiles({"mock-storage", "local", "mock-sam"})
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=5a9b583c-17ee-4c88-a14c-0edbf31175db",
      // source id must match value in WDS-integrationTest-LocalFileStorage-input.sql
      "twds.instance.source-workspace-id=10000000-0000-0000-0000-000000000111",
      "twds.pg_dump.useAzureIdentity=false"
    })
@DirtiesContext
@SpringBootTest
class InstanceInitializerCloneTest {

  // standard beans
  @Autowired InstanceInitializerBean instanceInitializerBean;
  @Autowired InstanceDao instanceDao;
  @Autowired RecordDao recordDao;
  @Autowired CloneDao cloneDao;
  @Autowired NamedParameterJdbcTemplate namedTemplate;

  // mock beans
  @MockBean WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;
  @MockBean LeonardoDao mockLeonardoDao;
  @MockBean WorkspaceManagerDao workspaceManagerDao;

  // values
  @Value("${twds.instance.workspace-id}")
  String workspaceId;

  @Value("${twds.instance.source-workspace-id}")
  String sourceWorkspaceId;

  @AfterEach
  void tearDown() {
    // clean up any instances left in the db
    List<UUID> allInstances = instanceDao.listInstanceSchemas();
    allInstances.forEach(instanceId -> instanceDao.dropSchema(instanceId));
    // clean up any clone entries
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.clone");
    // TODO: also drop any orphaned pg schemas that don't have an entry in the sys_wds.instances
    // table.
    // this can happen when restores fail.
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
    instanceInitializerBean.initializeInstance();

    // clone job should have errored, with friendly error message
    Job<CloneResponse> cloneStatus = cloneDao.getCloneStatus();
    assertSame(JobStatus.ERROR, cloneStatus.getStatus());
    assertSame(CloneStatus.BACKUPERROR, cloneStatus.getResult().status());
    assertEquals(
        "The data tables in the workspace being cloned do not support cloning. "
            + "Contact the workspace owner to upgrade the version of data tables in that workspace.",
        cloneStatus.getErrorMessage());

    // default instance should exist, with no tables in it
    UUID workspaceUuid = UUID.fromString(workspaceId);
    assertTrue(instanceDao.instanceSchemaExists(workspaceUuid));
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
    instanceInitializerBean.initializeInstance();

    // clone job should have succeeded
    Job<CloneResponse> cloneStatus = cloneDao.getCloneStatus();
    assertSame(JobStatus.SUCCEEDED, cloneStatus.getStatus());
    assertSame(CloneStatus.RESTORESUCCEEDED, cloneStatus.getResult().status());

    // default instance should exist, with a single table named "thing" in it
    // the "thing" table is defined in WDS-integrationTest-LocalFileStorage-input.sql.
    UUID workspaceUuid = UUID.fromString(workspaceId);
    List<UUID> actualInstances = instanceDao.listInstanceSchemas();
    assertEquals(List.of(workspaceUuid), actualInstances);
    List<RecordType> actualTypes = recordDao.getAllRecordTypes(workspaceUuid);
    assertEquals(List.of(RecordType.valueOf("thing")), actualTypes);
  }

  // TODO: if a clone entry already exists, initializeInstance won't do anything
  // TODO: test if backup succeeds but restore fails
  // TODO: what other coverage?

}
