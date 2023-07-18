package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedata.api.CloningApi;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceClientFactory;
import org.junit.jupiter.api.BeforeEach;
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

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ActiveProfiles({"mock-instance-dao", "mock-backup-dao", "mock-restore-dao", "local", "mock-sam"})
@TestPropertySource(properties = {
        "twds.instance.workspace-id=5a9b583c-17ee-4c88-a14c-0edbf31175db",
        "twds.instance.source-workspace-id=debc8737-8ff0-40c6-852b-3d4cdcdd2b74"})
@DirtiesContext
@SpringBootTest
class InstanceInitializerCloneTest {

    @Autowired
    InstanceInitializerBean instanceInitializerBean;

    @Autowired
    InstanceDao instanceDao;

    @Autowired
    RecordDao recordDao;

    @Autowired
    CloneDao cloneDao;

    @Autowired
    NamedParameterJdbcTemplate namedTemplate;

    @MockBean
    WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;

    @MockBean
    LeonardoDao mockLeonardoDao;

    @Value("${twds.instance.workspace-id}")
    String workspaceId;

    @Value("${twds.instance.source-workspace-id}")
    String sourceWorkspaceId;

    @BeforeEach
    void beforeEach() {
        // clean up any instances left in the db
        List<UUID> allInstances = instanceDao.listInstanceSchemas();
        allInstances.forEach(instanceId -> instanceDao.dropSchema(instanceId));
        // clean up any clone entries
        namedTemplate.getJdbcTemplate().update("delete from sys_wds.clone");
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
        given(mockLeonardoDao.getWdsEndpointUrl(any()))
                .willReturn("https://unit.test:7777");
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
        assertEquals("The data tables in the workspace being cloned do not support cloning. " +
                "Contact the workspace owner to upgrade the version of data tables in that workspace.",
                cloneStatus.getErrorMessage());

        // default instance should exist, with no tables in it
        UUID workspaceUuid = UUID.fromString(workspaceId);
        assertTrue(instanceDao.instanceSchemaExists(workspaceUuid));
        assertThat(recordDao.getAllRecordTypes(workspaceUuid)).isEmpty();
    }

}
