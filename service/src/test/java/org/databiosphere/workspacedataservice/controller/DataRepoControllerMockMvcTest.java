package org.databiosphere.workspacedataservice.controller;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import org.databiosphere.workspacedataservice.datarepo.DataRepoClientFactory;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class DataRepoControllerMockMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @InjectMocks
    @Autowired
    DataRepoDao dataRepoDao;

    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;

    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
    }


    private static String versionId = "v0.2";


    @Test
    void importSnapshotWithPermission() throws Exception {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(new SnapshotModel());
        UUID uuid = UUID.randomUUID();
        mockMvc.perform(post("/{instanceId}/snapshots/{version}/{snapshotId}", uuid, versionId, uuid)).andExpect(status().isAccepted());
    }

    @Test
    void importSnapshotWithoutPermission() throws Exception {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException(403, "Intentional error thrown for unit test"));
        UUID uuid = UUID.randomUUID();
        mockMvc.perform(post("/{instanceId}/snapshots/{version}/{snapshotId}", uuid, versionId, uuid)).andExpect(status().isForbidden());
    }
}
