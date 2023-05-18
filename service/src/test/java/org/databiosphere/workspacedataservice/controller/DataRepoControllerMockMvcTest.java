package org.databiosphere.workspacedataservice.controller;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import org.databiosphere.workspacedataservice.datarepo.DataRepoClientFactory;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ActiveProfiles(profiles = "mock-sam")
@SpringBootTest
@AutoConfigureMockMvc
class DataRepoControllerMockMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    DataRepoDao dataRepoDao;

    @Autowired
    WorkspaceManagerDao workspaceManagerDao;

    @MockBean
    DataRepoClientFactory mockDataRepoClientFactory;

    @MockBean
    WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;

    RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

    ReferencedGcpResourceApi mockReferencedGcpResourceApi = Mockito.mock(ReferencedGcpResourceApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
        given(mockWorkspaceManagerClientFactory.getReferencedGcpResourceApi()).willReturn(mockReferencedGcpResourceApi);
    }


    private static final String versionId = "v0.2";


    @Test
    void importSnapshotWithPermission() throws Exception {
        UUID uuid = UUID.randomUUID();
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willReturn(new SnapshotModel().id(uuid).name("foo"));
        given(mockReferencedGcpResourceApi.createDataRepoSnapshotReference(any(), any()))
            .willReturn(new DataRepoSnapshotResource());
        mockMvc.perform(post("/{instanceId}/snapshots/{version}/{snapshotId}", uuid, versionId, uuid)).andExpect(status().isAccepted());
    }

    @Test
    void importSnapshotWithoutPermission() throws Exception {
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
                .willThrow(new ApiException(403, "Intentional error thrown for unit test"));
        UUID uuid = UUID.randomUUID();
        mockMvc.perform(post("/{instanceId}/snapshots/{version}/{snapshotId}", uuid, versionId, uuid)).andExpect(status().isForbidden());
    }

    @Test
    void importSnapshotWithPolicyConflict() throws Exception {
        UUID uuid = UUID.randomUUID();
        given(mockRepositoryApi.retrieveSnapshot(any(), any()))
            .willReturn(new SnapshotModel().id(uuid).name("foo"));
        given(mockReferencedGcpResourceApi.createDataRepoSnapshotReference(any(), any()))
            .willThrow(new bio.terra.workspace.client.ApiException(409, "Policy Conflict"));
        mockMvc.perform(post("/{instanceId}/snapshots/{version}/{snapshotId}", uuid, versionId, uuid)).andExpect(status().isConflict());
    }
}
