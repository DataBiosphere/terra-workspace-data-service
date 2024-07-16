package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.shared.model.Collection;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
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
    Collection actualDb =
        namedTemplate.queryForObject(
            "select id, workspace_id, name, description from sys_wds.collection where workspace_id = :collectionId and name = :name",
            new MapSqlParameterSource(Map.of("workspaceId", workspaceId.id(), "name", name)),
            new CollectionRowMapper());

    assertNotNull(actualDb);
    assertEquals(workspaceId, actualDb.workspaceId(), "incorrect workspace id");
    assertEquals(collectionId, actualDb.collectionId(), "incorrect collection id");
    assertEquals(name, actualDb.name(), "incorrect collection name");
    assertEquals(description, actualDb.description(), "incorrect collection description");
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
    Collection actual =
        namedTemplate.queryForObject(
            "select id, workspace_id, name, description from sys_wds.collection where workspace_id = :collectionId and name = :name",
            new MapSqlParameterSource(Map.of("workspaceId", workspaceId.id(), "name", name)),
            new CollectionRowMapper());

    assertNotNull(actual);
    assertEquals(workspaceId, actual.workspaceId(), "incorrect workspace id");
    assertEquals(name, actual.name(), "incorrect collection name");
    assertEquals(description, actual.description(), "incorrect collection description");
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

  static class CollectionRowMapper implements RowMapper<Collection> {

    @Override
    public Collection mapRow(ResultSet rs, int rowNum) throws SQLException {
      CollectionId collectionId = CollectionId.of(UUID.fromString(rs.getString("id")));
      WorkspaceId workspaceId = WorkspaceId.of(UUID.fromString(rs.getString("workspace_id")));
      String name = rs.getString("name");
      String description = rs.getString("description");
      return new Collection(workspaceId, collectionId, name, description);
    }
  }
}
