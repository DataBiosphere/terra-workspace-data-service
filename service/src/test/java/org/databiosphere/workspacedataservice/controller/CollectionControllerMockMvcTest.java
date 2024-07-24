package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.databiosphere.workspacedata.model.ErrorResponse;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@TestPropertySource(properties = {"twds.tenancy.enforce-collections-match-workspace-id=false"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CollectionControllerMockMvcTest extends MockMvcTestBase {

  @Autowired private ObjectMapper objectMapper;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private CollectionService collectionService;

  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  // delete all collections, across all workspaces
  private void cleanupAll() {
    List<UUID> collIds =
        namedTemplate.queryForList(
            "select distinct id from sys_wds.collection", Map.of(), UUID.class);
    collIds.forEach(
        collId -> {
          CollectionId collectionId = CollectionId.of(collId);
          namedTemplate
              .getJdbcTemplate()
              .update("drop schema " + quote(collectionId.toString()) + " cascade");
        });
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.collection");
  }

  @BeforeEach
  void beforeEach() {
    cleanupAll();
  }

  @AfterAll
  void afterAll() {
    cleanupAll();
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
        new CollectionServerModel("different-name", "different description");
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

  @Disabled("for a future PR")
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
        "403 FORBIDDEN \"Caller does not have permission to create collection.\"",
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
    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void deleteCollection() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // now delete it
    mockMvc
        .perform(delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
        .andExpect(status().isNoContent())
        .andReturn();

    // assert collection no longer exists
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void deleteNonexistentCollectionReadPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // mock read-only permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // this test has two failure conditions:
    // 1) the user doesn't have permissions to delete collections
    // 2) the collection doesn't exist
    // our code checks the first condition first, so that's the error we expect below

    // attempt to delete a random UUID
    MvcResult mvcResult =
        mockMvc
            .perform(
                delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"Caller does not have permission to delete collections from this workspace.\"",
        mvcResult.getResolvedException().getMessage());
  }

  @Test
  void deleteNonexistentCollectionNoPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // mock no permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    // attempt to delete a random UUID
    MvcResult mvcResult =
        mockMvc
            .perform(
                delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isNotFound())
            .andReturn();

    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());
  }

  @Test
  void deleteCollectionReadonlyWorkspacePermission() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // mock read-only permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // now delete it
    MvcResult mvcResult =
        mockMvc
            .perform(
                delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"Caller does not have permission to delete collections from this workspace.\"",
        mvcResult.getResolvedException().getMessage());

    // assert collection was not deleted
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void deleteCollectionNoWorkspacePermission() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // mock no permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    // now delete it
    MvcResult mvcResult =
        mockMvc
            .perform(
                delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isNotFound())
            .andReturn();

    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());

    // assert collection was not deleted
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void deleteCollectionWithMismatchedWorkspaceId() throws Exception {
    // create a collection as setup, so we can ensure it deletes
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    WorkspaceId otherWorkspaceId = WorkspaceId.of(UUID.randomUUID());

    // user has write permissions on both workspaces
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(otherWorkspaceId).thenReturn(true);

    // attempt to delete it, but specify the wrong workspaceId in the request
    MvcResult mvcResult =
        mockMvc
            .perform(
                delete(
                    "/collections/v1/{workspaceId}/{collectionId}", otherWorkspaceId, collectionId))
            .andExpect(status().isBadRequest())
            .andReturn();

    assertInstanceOf(ValidationException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection does not belong to the specified workspace",
        mvcResult.getResolvedException().getMessage());

    // assert collection was not deleted
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void listCollections() throws Exception {
    var testSize = 4;
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // mock read permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
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
    // create a collection as setup; it should not be returned in the list
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    insertCollection(workspaceId);

    // mock no permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    // list collections
    MvcResult mvcResult =
        mockMvc
            .perform(get("/collections/v1/{workspaceId}", workspaceId))
            .andExpect(status().isNotFound())
            .andReturn();

    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());
  }

  @Test
  void getCollection() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure read permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // attempt to get the collection
    MvcResult mvcResult =
        mockMvc
            .perform(get("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isOk())
            .andReturn();

    CollectionServerModel actual =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), CollectionServerModel.class);

    assertEquals(collectionServerModel, actual);
  }

  @Test
  void getCollectionNoPermission() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure no permission
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // attempt to get the collection
    MvcResult mvcResult =
        mockMvc
            .perform(get("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))
            .andExpect(status().isNotFound())
            .andReturn();

    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());
  }

  @Test
  void getNonexistentCollection() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // ensure read permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    // attempt to get a nonexistent collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                get("/collections/v1/{workspaceId}/{collectionId}", workspaceId, UUID.randomUUID()))
            .andExpect(status().isNotFound())
            .andReturn();

    assertInstanceOf(MissingObjectException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection does not exist or you do not have permission to see it",
        mvcResult.getResolvedException().getMessage());
  }

  @Test
  void getNonexistentCollectionNoPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // ensure no permission
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // attempt to get a nonexistent collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                get("/collections/v1/{workspaceId}/{collectionId}", workspaceId, UUID.randomUUID()))
            .andExpect(status().isNotFound())
            .andReturn();

    // this test has two failure conditions:
    // 1) the user doesn't have permissions to read collections (i.e. read the workspace)
    // 2) the collection doesn't exist
    // our code checks the first condition first, so that's the error we expect below

    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());
  }

  // collection does exist, but in a different workspace
  @Test
  void getCollectionMismatchedWorkspaceId() throws Exception {
    WorkspaceId workspaceIdOne = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId workspaceIdTwo = WorkspaceId.of(UUID.randomUUID());

    // create a collection as setup, in workspace one
    CollectionServerModel collectionServerModel = insertCollection(workspaceIdOne);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure read permission on workspace two
    stubReadWorkspacePermission(workspaceIdTwo).thenReturn(true);

    // attempt to get the collection from workspace two (collection belongs to workspace one)
    MvcResult mvcResult =
        mockMvc
            .perform(
                get("/collections/v1/{workspaceId}/{collectionId}", workspaceIdTwo, collectionId))
            .andExpect(status().isNotFound())
            .andReturn();

    assertInstanceOf(MissingObjectException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection does not exist or you do not have permission to see it",
        mvcResult.getResolvedException().getMessage());
  }

  @Test
  void getCollectionMismatchedWorkspaceIdNoPermission() throws Exception {
    WorkspaceId workspaceIdOne = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId workspaceIdTwo = WorkspaceId.of(UUID.randomUUID());

    // create a collection as setup, in workspace one
    CollectionServerModel collectionServerModel = insertCollection(workspaceIdOne);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure no permission on workspace two
    stubReadWorkspacePermission(workspaceIdTwo).thenReturn(false);
    stubWriteWorkspacePermission(workspaceIdTwo).thenReturn(false);

    // attempt to get the collection from workspace two (collection belongs to workspace one)
    MvcResult mvcResult =
        mockMvc
            .perform(
                get("/collections/v1/{workspaceId}/{collectionId}", workspaceIdTwo, collectionId))
            .andExpect(status().isNotFound())
            .andReturn();

    // this test has two failure conditions:
    // 1) the user doesn't have permissions to read collections (i.e. read the workspace)
    // 2) the collection doesn't exist in workspace two
    // our code checks the first condition first, so that's the error we expect below

    AuthenticationMaskableException actual =
        assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
    assertEquals("Workspace", actual.getObjectType());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals(
        "Workspace does not exist or you do not have permission to see it",
        errorResponse.getMessage());
  }

  // ==================== test utilities

  private CollectionServerModel insertCollection(WorkspaceId workspaceId) {
    return insertCollection(workspaceId, "collection-name", "collection description");
  }

  private CollectionServerModel insertCollection(
      WorkspaceId workspaceId, String name, String description) {
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    return insertCollection(workspaceId, collectionId, name, description);
  }

  private CollectionServerModel insertCollection(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    // determine current write permission, if any
    boolean originalWritePermission = false;
    if (samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId) != null) {
      originalWritePermission =
          samAuthorizationDaoFactory
              .getSamAuthorizationDao(workspaceId)
              .hasWriteWorkspacePermission();
    }

    // mock write permission, save the collection, and assert it created
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);
    collectionService.save(workspaceId, collectionServerModel);
    assertCollectionExists(workspaceId, collectionId, name, description);

    // reset write permission to original state
    stubWriteWorkspacePermission(workspaceId).thenReturn(originalWritePermission);

    return collectionServerModel;
  }

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
