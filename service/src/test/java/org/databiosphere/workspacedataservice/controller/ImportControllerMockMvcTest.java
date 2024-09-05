package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = {"mock-instance-dao", "mock-sam"})
@DirtiesContext
class ImportControllerMockMvcTest extends MockMvcTestBase {

  @Autowired private CollectionService collectionService;
  @Autowired private TwdsProperties twdsProperties;

  @Test
  void smokeTestCreateImport() throws Exception {
    CollectionServerModel collection =
        collectionService.save(twdsProperties.workspaceId(), "name", "description");
    CollectionId collectionId = CollectionId.of(collection.getId());
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB,
            new URI("https://teststorageaccount.blob.core.windows.net/testcontainer/file"));

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
