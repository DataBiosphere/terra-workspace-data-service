package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.TableModel;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
class DataRepoDaoTest {

    @Autowired
    DataRepoDao dataRepoDao;

    @Autowired
    RecordDao recordDao;

    @Autowired
    InstanceDao instanceDao;

    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;

    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    private static final UUID INSTANCE = UUID.fromString("111e9999-e89b-12d3-a456-426614174000");

    @BeforeEach
    void beforeEach() {
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
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
    void testSnapshotReturned() throws ApiException {
        final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(UUID.randomUUID());
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(testSnapshot);
        assertEquals(testSnapshot, dataRepoDao.getSnapshot(testSnapshot.getId()));
        Mockito.clearInvocations(mockRepositoryApi);
    }

    @Test
    void testErrorThrown() throws ApiException {
        final int statusCode = HttpStatus.UNAUTHORIZED.value();
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException(statusCode, "Intentional error thrown for unit test"));
        var exception = assertThrows(DataRepoException.class, () -> dataRepoDao.getSnapshot(UUID.randomUUID()));
        assertEquals(statusCode, exception.getRawStatusCode());
        Mockito.clearInvocations(mockRepositoryApi);
    }

    @Test
    void testAddSnapshot() {
        List<TableModel> tables = new ArrayList<>();
        for (int i = 0; i < 3; i++){
            tables.add(new TableModel().name("table"+(i+1)));
        }
        final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(UUID.randomUUID()).tables(tables);

        dataRepoDao.addSnapshot(testSnapshot, INSTANCE);

        assertTrue(recordDao.recordTypeExists(INSTANCE, RecordType.valueOf(DataRepoDao.TDRIMPORT_TABLE)));
        List<org.databiosphere.workspacedataservice.shared.model.Record> result = recordDao.queryForRecords(RecordType.valueOf(DataRepoDao.TDRIMPORT_TABLE),3,0,"asc",null,INSTANCE);
        for (int i = 0; i < 3; i++){
            assertEquals(result.get(i).getId(), "table"+(i+1));
        }
    }
}
