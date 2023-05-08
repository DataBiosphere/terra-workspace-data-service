package org.databiosphere.workspacedataservice.datarepo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@SpringBootTest(
        classes = {DataRepoConfig.class})
class DataRepoDaoTest {

    @Autowired
    DataRepoDao dataRepoDao;

    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;

    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
    }

    @Test
    void testSnapshotReturned() throws ApiException {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(new SnapshotModel());
        assert(dataRepoDao.hasSnapshotPermission(UUID.randomUUID()));
    }

    @Test
    void testUnauthorizedErrorThrown() throws ApiException {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException(401, "Intentional error thrown for unit test"));
        assertFalse(dataRepoDao.hasSnapshotPermission(UUID.randomUUID()));
    }

    @Test
    void testForbiddenErrorThrown() throws ApiException {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException(403, "Intentional error thrown for unit test"));
        assertFalse(dataRepoDao.hasSnapshotPermission(UUID.randomUUID()));
    }

    @Test
    void testErrorThrown() throws ApiException {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException());
        assertThrows(ApiException.class,
                () -> dataRepoDao.hasSnapshotPermission(UUID.randomUUID()),
                "dataRepoDao should rethrow non-401 or 403 exception");
    }

}
