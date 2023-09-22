package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.datarepo.DataRepoClientFactory;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerClientFactory;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest
@AutoConfigureMockMvc
class DataRepoControllerMockMvcTest {

  @Autowired private ObjectMapper mapper;

  @Autowired private MockMvc mockMvc;

  @Autowired DataRepoDao dataRepoDao;

  @Autowired WorkspaceManagerDao workspaceManagerDao;

  @MockBean DataRepoClientFactory mockDataRepoClientFactory;

  @MockBean WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;

  final RepositoryApi mockRepositoryApi = Mockito.mock(RepositoryApi.class);

  final ReferencedGcpResourceApi mockReferencedGcpResourceApi =
      Mockito.mock(ReferencedGcpResourceApi.class);

  private static UUID instanceId;

  private static final String versionId = "v0.2";

  @BeforeEach
  void beforeEach() throws Exception {
    given(mockDataRepoClientFactory.getRepositoryApi()).willReturn(mockRepositoryApi);
    given(mockWorkspaceManagerClientFactory.getReferencedGcpResourceApi(null))
        .willReturn(mockReferencedGcpResourceApi);
    instanceId = UUID.randomUUID();
    mockMvc
        .perform(post("/instances/{v}/{instanceid}", versionId, instanceId).content(""))
        .andExpect(status().isCreated());
  }

  @AfterEach
  void afterEach() {
    try {
      mockMvc
          .perform(delete("/instances/{v}/{instanceid}", versionId, instanceId).content(""))
          .andExpect(status().isOk());
    } catch (Throwable t) {
      // noop - if we fail to delete the instance, don't fail the test
    }
  }

  @Test
  void importSnapshotWithPermission() throws Exception {
    UUID uuid = UUID.randomUUID();
    given(mockRepositoryApi.retrieveSnapshot(any(), any()))
        .willReturn(
            new SnapshotModel()
                .id(uuid)
                .name("foo")
                .tables(List.of(new TableModel().name("table1"), new TableModel().name("table2"))));
    given(mockReferencedGcpResourceApi.createDataRepoSnapshotReference(any(), any()))
        .willReturn(new DataRepoSnapshotResource());
    mockMvc
        .perform(
            post("/{instanceId}/snapshots/{version}/{snapshotId}", instanceId, versionId, uuid))
        .andExpect(status().isAccepted());

    // validate import
    MvcResult result =
        mockMvc
            .perform(
                post(
                    "/{instanceId}/search/{version}/{recordType}",
                    instanceId,
                    versionId,
                    DataRepoService.TDRIMPORT_TABLE))
            .andExpect(status().isOk())
            .andReturn();

    RecordQueryResponse response =
        mapper.readValue(result.getResponse().getContentAsString(), RecordQueryResponse.class);
    assertEquals(2, response.totalRecords());
    assertEquals("table1", response.records().get(0).recordId());
  }

  @Test
  void importSnapshotWithoutPermission() throws Exception {
    given(mockRepositoryApi.retrieveSnapshot(any(), any()))
        .willThrow(new ApiException(403, "Intentional error thrown for unit test"));
    UUID uuid = UUID.randomUUID();
    mockMvc
        .perform(
            post("/{instanceId}/snapshots/{version}/{snapshotId}", instanceId, versionId, uuid))
        .andExpect(status().isForbidden());
  }

  @Test
  void importSnapshotWithPolicyConflict() throws Exception {
    UUID uuid = UUID.randomUUID();
    given(mockRepositoryApi.retrieveSnapshot(any(), any()))
        .willReturn(new SnapshotModel().id(uuid).name("foo"));
    given(mockReferencedGcpResourceApi.createDataRepoSnapshotReference(any(), any()))
        .willThrow(new bio.terra.workspace.client.ApiException(409, "Policy Conflict"));
    mockMvc
        .perform(
            post("/{instanceId}/snapshots/{version}/{snapshotId}", instanceId, versionId, uuid))
        .andExpect(status().isConflict());
  }
}
