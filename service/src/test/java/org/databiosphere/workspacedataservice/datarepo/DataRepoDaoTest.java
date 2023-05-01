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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@SpringBootTest(
        classes = {DataRepoConfig.class})
public class DataRepoDaoTest {

    @Autowired
    DataRepoDao dataRepoDao;

    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;

    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
        Mockito.clearInvocations(mockRepositoryApi);
    }

    @Test
    public void testSnapshotReturned() throws ApiException {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(new SnapshotModel());
        assert(dataRepoDao.hasSnapshotPermission(UUID.randomUUID()));
    }

    @Test
    public void testErrorThrown() throws ApiException {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException());
        assertFalse(dataRepoDao.hasSnapshotPermission(UUID.randomUUID()));
    }

}
