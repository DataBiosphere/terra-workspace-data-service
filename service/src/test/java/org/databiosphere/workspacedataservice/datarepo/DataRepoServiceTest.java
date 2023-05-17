package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"}) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
class DataRepoServiceTest {

    @Autowired
    DataRepoService dataRepoService;

    @Autowired
    RecordDao recordDao;

    @Autowired
    InstanceDao instanceDao;

    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;

    @Autowired
    WorkspaceManagerDao workspaceManagerDao;

    @MockBean
    WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;

    ReferencedGcpResourceApi mockReferencedGcpResourceApi = Mockito.mock(ReferencedGcpResourceApi.class);

    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    private static final UUID INSTANCE = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @BeforeEach
    void beforeEach() {
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
        given(mockWorkspaceManagerClientFactory.getReferencedGcpResourceApi()).willReturn(mockReferencedGcpResourceApi);
        if (!instanceDao.instanceSchemaExists(INSTANCE)) {
            instanceDao.createSchema(INSTANCE);
        }
    }
    @AfterEach
    void afterEach() {
        if (instanceDao.instanceSchemaExists(INSTANCE)) {
            instanceDao.dropSchema(INSTANCE);
        }
    }

    @Test
    void testImportSnapshot() throws ApiException {
        UUID snapshotId = UUID.randomUUID();
        List<TableModel> tables = new ArrayList<>();
        for (int i = 0; i < 3; i++){
            tables.add(new TableModel().name("table"+(i+1)));
        }
        final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(snapshotId).tables(tables);
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(testSnapshot);

        dataRepoService.importSnapshot(INSTANCE, snapshotId);

        assertTrue(recordDao.recordTypeExists(INSTANCE, RecordType.valueOf(DataRepoDao.TDRIMPORT_TABLE)));
        List<org.databiosphere.workspacedataservice.shared.model.Record> result = recordDao.queryForRecords(RecordType.valueOf(DataRepoDao.TDRIMPORT_TABLE),3,0,"asc",null,INSTANCE);
        for (int i = 0; i < 3; i++){
            assertEquals(result.get(i).getId(), "table"+(i+1));
        }
    }
}
