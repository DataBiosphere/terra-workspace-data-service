package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = {"mock-instance-dao", "mock-sam"})
@DirtiesContext
@TestPropertySource(properties = "twds.data-import.connectivity-check-enabled=false")
class ImportControllerMockMvcTest extends MockMvcTestBase {

  @Autowired private CollectionService collectionService;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired WorkspaceRepository workspaceRepository;

  @AfterEach
  void afterEach() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  @Test
  void smokeTestCreateImport() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // create the workspace record
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));
    CollectionServerModel collection = collectionService.save(workspaceId, "name", "description");
    CollectionId collectionId = CollectionId.of(collection.getId());
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB,
            new URI("https://anvil.gi.ucsc.edu/testcontainer/file"));

    // calling the API should result in 201 Created
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/{instanceUuid}/import/v1", collectionId)
                    .content(toJson(importRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isAccepted())
            .andReturn();

    // and the API response should be a valid GenericJobServerModel
    assertDoesNotThrow(() -> fromJson(mvcResult, GenericJobServerModel.class));
  }
}
