package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CollectionControllerMockMvcTest extends MockMvcTestBase {

  // TODO I think this version will go up
  private static final String versionId = "v0.2";

  private final UUID collectionId = UUID.randomUUID();

  @Test
  @Transactional
  void createCollectionAndTryToCreateAgain() throws Exception {
    UUID uuid = UUID.randomUUID();
    mockMvc
        .perform(post("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isCreated());
    mockMvc
        .perform(post("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isConflict());
  }

  @Test
  void listCollections() throws Exception {
    // get initial collection list
    MvcResult initialResult =
        mockMvc
            .perform(get("/collections/{version}", versionId))
            .andExpect(status().isOk())
            .andReturn();
    UUID[] initialCollections = fromJson(initialResult, UUID[].class);
    // create new uuid; new uuid should not be in our initial collection list
    UUID uuid = UUID.randomUUID();
    assertFalse(
        Arrays.asList(initialCollections).contains(uuid),
        "initial collection list should not contain brand new UUID");
    // create a new collection from the new uuid
    mockMvc
        .perform(post("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isCreated());
    // get collection list again
    MvcResult afterCreationResult =
        mockMvc
            .perform(get("/collections/{version}", versionId))
            .andExpect(status().isOk())
            .andReturn();
    UUID[] afterCreationCollections = fromJson(afterCreationResult, UUID[].class);
    // new uuid should be in our initial collection list
    assertTrue(
        Arrays.asList(afterCreationCollections).contains(uuid),
        "after-creation collection list should contain brand new UUID");

    assertEquals(
        initialCollections.length + 1,
        afterCreationCollections.length,
        "size of after-creation list should be equal to the initial size plus one");
  }

  @Test
  @Transactional
  void deleteCollection() throws Exception {
    UUID uuid = UUID.randomUUID();
    // delete nonexistent collection should 404
    mockMvc
        .perform(delete("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isNotFound());
    // creating the collection should 201
    mockMvc
        .perform(post("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isCreated());
    // delete existing collection should 200
    mockMvc
        .perform(delete("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isOk());
    // deleting again should 404
    mockMvc
        .perform(delete("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isNotFound());
    // creating again should 201
    mockMvc
        .perform(post("/collections/{version}/{collectionId}", versionId, uuid))
        .andExpect(status().isCreated());
  }

  @Test
  @Transactional
  void deleteCollectionContainingData() throws Exception {
    // Create collection
    mockMvc
        .perform(post("/collections/{v}/{collectionid}", versionId, collectionId).content(""))
        .andExpect(status().isCreated());

    RecordAttributes attributes = new RecordAttributes(Map.of("foo", "bar", "num", 123));
    // create "to" record, which will be the target of a relation
    mockMvc
        .perform(
            put(
                    "/{collectionId}/records/{version}/{recordType}/{recordId}",
                    collectionId,
                    versionId,
                    "to",
                    "1")
                .content(toJson(new RecordRequest(attributes)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    // create "from" record, with a relation to "to"
    RecordAttributes attributes2 = new RecordAttributes(Map.of("relation", "terra-wds:/to/1"));
    mockMvc
        .perform(
            put(
                    "/{collectionId}/records/{version}/{recordType}/{recordId}",
                    collectionId,
                    versionId,
                    "from",
                    "2")
                .content(toJson(new RecordRequest(attributes2)))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
    // delete existing collection should 200
    mockMvc
        .perform(delete("/collections/{version}/{collectionId}", versionId, collectionId))
        .andExpect(status().isOk());
  }
}
