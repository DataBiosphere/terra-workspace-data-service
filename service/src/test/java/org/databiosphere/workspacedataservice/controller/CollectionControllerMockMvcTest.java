package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
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
  @Autowired private CollectionService collectionService;

  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  /* TODO: this test causes other unit tests to fail.
      A WDS "collection" is two things: a row in the sys_wds.collection table, and a Postgres
      schema.
      Because I haven't fully implemented CollectionService yet, we are adding rows to the
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

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

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

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

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

  @Test
  void createCollectionIdConflict() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // create the collection
    collectionService.save(workspaceId, collectionServerModel);
    // assert it created
    assertCollectionExists(workspaceId, collectionId, name, description);

    // generate a new collection request with a different name/description but the same id
    CollectionServerModel conflictRequest =
        new CollectionServerModel("different name", "different description");
    conflictRequest.id(collectionId.id());

    // attempt to create the same id again via the API; should result in 409 Conflict
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(conflictRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isConflict())
            .andReturn();

    // verify error message
    assertInstanceOf(ConflictException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection with this id already exists", mvcResult.getResolvedException().getMessage());

    // original collection should remain untouched
    assertCollectionExists(workspaceId, collectionId, name, description);
  }

  @Test
  void createCollectionNameConflict() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // create the collection
    collectionService.save(workspaceId, collectionServerModel);
    // assert it created
    assertCollectionExists(workspaceId, collectionId, name, description);

    // generate a new collection request with a different id and description but the same name
    CollectionId conflictId = CollectionId.of(UUID.randomUUID());
    CollectionServerModel conflictRequest =
        new CollectionServerModel(name, "different description");
    conflictRequest.id(conflictId.id());

    // attempt to create the same id again via the API; should result in 409 Conflict
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(conflictRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isConflict())
            .andReturn();

    // verify error message
    assertInstanceOf(ConflictException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection with this name already exists in this workspace",
        mvcResult.getResolvedException().getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(conflictId);
    // original collection should remain untouched
    assertCollectionExists(workspaceId, collectionId, name, description);
  }

  @Test
  void createCollectionInvalidName() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "whoa! this is an illegal name with spaces and an exclamation mark";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // create the collection
    // attempt to create the same id again via the API; should result in 409 Conflict
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isBadRequest())
            .andReturn();

    // verify error message
    assertInstanceOf(ConflictException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection with this id already exists", mvcResult.getResolvedException().getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void createCollectionReadonlyWorkspacePermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    // verify error message
    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "TODO: message about requiring write permission",
        mvcResult.getResolvedException().getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void createCollectionNoWorkspacePermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andReturn();

    // verify error message
    assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals(
        "Workspace not found or you do not have permission to use it",
        mvcResult.getResolvedException().getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void deleteCollection() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    collectionService.save(workspaceId, collectionServerModel);

    // assert it was created correctly
    assertCollectionExists(workspaceId, collectionId, name, description);

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // now delete it
    mockMvc
        .perform(delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
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

  @Test
  void deleteNonexistentCollectionReadPemission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // mock read-only permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // attempt to delete a random UUID
    mockMvc
        .perform(delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
        .andExpect(status().isNotFound())
        .andReturn();
  }

  @Test
  void deleteNonexistentCollectionNoPemission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // mock no permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    // attempt to delete a random UUID
    mockMvc
        .perform(delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
        .andExpect(status().isNotFound())
        .andReturn();
  }

  @Test
  void deleteCollectionReadonlyWorkspacePermission() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    // mock read-only permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    collectionService.save(workspaceId, collectionServerModel);

    // assert it was created correctly
    assertCollectionExists(workspaceId, collectionId, name, description);

    // now delete it
    mockMvc
        .perform(delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
        .andExpect(status().isForbidden())
        .andReturn();

    // assert collection was not deleted
    assertCollectionExists(workspaceId, collectionId, name, description);
  }

  @Test
  void deleteCollectionNoWorkspacePermission() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    // mock no permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    collectionService.save(workspaceId, collectionServerModel);

    // assert it was created correctly
    assertCollectionExists(workspaceId, collectionId, name, description);

    // now delete it
    mockMvc
        .perform(delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
        .andExpect(status().isNotFound())
        .andReturn();

    // assert collection was not deleted
    assertCollectionExists(workspaceId, collectionId, name, description);
  }

  @Test
  void listCollections() throws Exception {
    var testSize = 4;
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // mock read permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    for (int i = 1; i < testSize; i++) {
      // create another collection
      CollectionId collectionId = CollectionId.of(UUID.randomUUID());
      String name = "test-name-" + i;
      String description = "unit test description " + i;

      CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
      collectionServerModel.id(collectionId.id());

      collectionService.save(workspaceId, collectionServerModel);

      // assert it was created correctly
      assertCollectionExists(workspaceId, collectionId, name, description);

      // list collections
      MvcResult mvcResult =
          mockMvc
              .perform(get("/collections/v1/{workspaceId}", workspaceId))
              .andExpect(status().isOk())
              .andReturn();

      List<CollectionServerModel> collectionServerModelList =
          objectMapper.readValue(
              mvcResult.getResponse().getContentAsString(), new TypeReference<>() {});

      assertEquals(i, collectionServerModelList.size());
      List<UUID> ids =
          collectionServerModelList.stream().map(CollectionServerModel::getId).toList();
      assertThat(ids).contains(collectionId.id());
    }
  }

  @Test
  void listCollectionsEmpty() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // mock read-only permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // list collections
    MvcResult mvcResult =
        mockMvc
            .perform(get("/collections/v1/{workspaceId}", workspaceId))
            .andExpect(status().isOk())
            .andReturn();

    List<CollectionServerModel> collectionServerModelList =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), new TypeReference<>() {});

    assertEquals(0, collectionServerModelList.size());
  }

  @Test
  void listCollectionsNoWorkspacePermission() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    // mock no permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    collectionService.save(workspaceId, collectionServerModel);

    // assert it was created correctly
    assertCollectionExists(workspaceId, collectionId, name, description);

    // now delete it
    mockMvc
        .perform(get("/collections/v1/{workspaceId}", workspaceId))
        .andExpect(status().isNotFound())
        .andReturn();
  }

  // ==================== test utilities

  private void assertCollectionExists(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    CollectionId actualCollectionId = assertCollectionExists(workspaceId, name, description);

    assertEquals(collectionId, actualCollectionId, "incorrect collection id");
  }

  private CollectionId assertCollectionExists(
      WorkspaceId workspaceId, String name, String description) {
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
    return actual.collectionId();
  }

  private void assertCollectionDoesNotExist(CollectionId collectionId) {
    assertThrows(
        Exception.class,
        () ->
            namedTemplate.queryForObject(
                "select id, workspace_id, name, description from sys_wds.collection where id = :collectionId",
                new MapSqlParameterSource(Map.of("collectionId", collectionId.id())),
                new CollectionRowMapper()));
  }

  private OngoingStubbing<Boolean> stubReadWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasReadWorkspacePermission());
  }

  private OngoingStubbing<Boolean> stubWriteWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasWriteWorkspacePermission());
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
