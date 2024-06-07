package org.databiosphere.workspacedataservice.controller;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
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

  @Autowired private CollectionDao collectionDao;

  @Test
  void smokeTestCreateImport() throws Exception {
    CollectionId collectionId = CollectionId.of(randomUUID());
    collectionDao.createSchema(collectionId);
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
