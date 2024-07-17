package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.CollectionServiceV1;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
public class CollectionControllerMockMvcTest extends MockMvcTestBase {

  @Autowired private ObjectMapper objectMapper;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private CollectionServiceV1 collectionServiceV1;

  /* TODO: this test causes other unit tests to fail.
      A WDS "collection" is two things: a row in the sys_wds.collection table, and a Postgres
      schema.
      Because I haven't fully implemented CollectionServiceV1 yet, we are adding rows to the
      collection table but not creating the schemas. This gets things out of sync and other
      tests have a problem with that.
  */

  @BeforeEach
  void beforeEach() {
    // empty out the collection table so each test starts fresh
    namedTemplate.update("delete from sys_wds.collection", Map.of());
  }

  @Test
  // create a collection, specifying the collection id in the request
  void createCollectionWithId() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    // calling the API should result in 201 Created
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();

    CollectionServerModel actualResponse =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), CollectionServerModel.class);

    assertNotNull(actualResponse);
    assertEquals(collectionId.id(), actualResponse.getId(), "incorrect collection id");
    assertEquals(name, actualResponse.getName(), "incorrect collection name");
    assertEquals(description, actualResponse.getDescription(), "incorrect collection description");

    // the collection should exist in the db
    assertCollectionExists(workspaceId, collectionId, name, description);
  }

  @Test
  // create a collection without specifying the collection id in the request
  void createCollectionWithoutId() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);

    // calling the API should result in 201 Created
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isCreated())
            .andReturn();

    CollectionServerModel actualResponse =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), CollectionServerModel.class);

    assertNotNull(actualResponse);
    assertNotNull(actualResponse.getId(), "collection id should not be null");
    assertEquals(name, actualResponse.getName(), "incorrect collection name");
    assertEquals(description, actualResponse.getDescription(), "incorrect collection description");

    // the collection should exist in the db
    assertCollectionExists(workspaceId, name, description);
  }

  @Disabled
  @Test
  void createCollectionIdConflict() {}

  @Disabled
  @Test
  void createCollectionNameConflict() {}

  @Disabled
  @Test
  void createCollectionInvalidName() {}

  @Disabled
  @Test
  void createCollectionReadonlyWorkspacePermission() {}

  @Disabled
  @Test
  void createCollectionNoWorkspacePermission() {}

  @Test
  void deleteCollection() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    collectionServiceV1.save(workspaceId, collectionServerModel);

    // assert it was created correctly
    assertCollectionExists(workspaceId, collectionId, name, description);

    // now delete it
    MvcResult mvcResult =
        mockMvc
            .perform(
                delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isNoContent())
            .andReturn();

    // assert collection no longer exists
    assertFalse(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.collection where id = :collectionId)",
            new MapSqlParameterSource(Map.of("collectionId", collectionId.id())),
            Boolean.class),
        "collection should no longer exist");
  }

  private void assertCollectionExists(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    WdsCollection actual =
        namedTemplate.queryForObject(
            "select id, workspace_id, name, description from sys_wds.collection where workspace_id = :workspaceId and name = :name",
            new MapSqlParameterSource(Map.of("workspaceId", workspaceId.id(), "name", name)),
            new CollectionRowMapper());

    assertNotNull(actual);
    assertEquals(collectionId, actual.collectionId(), "incorrect collection id");
    assertEquals(workspaceId, actual.workspaceId(), "incorrect workspace id");
    assertEquals(name, actual.name(), "incorrect collection name");
    assertEquals(description, actual.description(), "incorrect collection description");
  }

  private void assertCollectionExists(WorkspaceId workspaceId, String name, String description) {
    WdsCollection actual =
        namedTemplate.queryForObject(
            "select id, workspace_id, name, description from sys_wds.collection where workspace_id = :workspaceId and name = :name",
            new MapSqlParameterSource(Map.of("workspaceId", workspaceId.id(), "name", name)),
            new CollectionRowMapper());

    assertNotNull(actual);
    assertNotNull(actual.collectionId(), "collection id should not be null");
    assertEquals(workspaceId, actual.workspaceId(), "incorrect workspace id");
    assertEquals(name, actual.name(), "incorrect collection name");
    assertEquals(description, actual.description(), "incorrect collection description");
  }

  static class CollectionRowMapper implements RowMapper<WdsCollection> {
    @Override
    public WdsCollection mapRow(ResultSet rs, int rowNum) throws SQLException {
      CollectionId collectionId = CollectionId.of(UUID.fromString(rs.getString("id")));
      WorkspaceId workspaceId = WorkspaceId.of(UUID.fromString(rs.getString("workspace_id")));
      String name = rs.getString("name");
      String description = rs.getString("description");
      return new WdsCollection(workspaceId, collectionId, name, description);
    }
  }
}
