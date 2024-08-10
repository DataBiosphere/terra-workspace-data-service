package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.databiosphere.workspacedata.model.ErrorResponse;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
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
import org.springframework.web.bind.MethodArgumentNotValidException;

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
  void createCollection() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // calling the API should result in 201 Created
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionRequestServerModel))
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
  void createCollectionNameConflict() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // create the collection
    CollectionServerModel actual =
        collectionService.save(workspaceId, collectionRequestServerModel);
    // assert it created
    assertCollectionExists(workspaceId, actual.getId(), name, description);

    // generate a new collection request with a different id and description but the same name
    CollectionId conflictId = CollectionId.of(UUID.randomUUID());
    CollectionRequestServerModel conflictRequest =
        new CollectionRequestServerModel(name, "different description");

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
    assertCollectionExists(workspaceId, actual.getId(), name, description);
  }

  @Test
  void createCollectionInvalidName() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    String name = "whoa! this is an illegal name with spaces and an exclamation mark";
    String description = "unit test description";

    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // create the collection
    // attempt to create the same id again via the API; should result in 409 Conflict
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionRequestServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isBadRequest())
            .andReturn();

    // verify error message
    assertInstanceOf(MethodArgumentNotValidException.class, mvcResult.getResolvedException());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals("name: must match \"[a-zA-Z0-9-_]{1,128}\"", errorResponse.getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(name);
  }

  @Test
  void createCollectionReadonlyWorkspacePermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(true);

    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionRequestServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    // verify error message
    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
        mvcResult.getResolvedException().getMessage());

    // new collection should not have been created
    assertCollectionDoesNotExist(name);
  }

  @Test
  void createCollectionNoWorkspacePermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

    stubWriteWorkspacePermission(workspaceId).thenReturn(false);
    stubReadWorkspacePermission(workspaceId).thenReturn(false);

    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionRequestServerModel))
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
    assertCollectionDoesNotExist(name);
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
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
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
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
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
      String name = "test-name-" + i;
      String description = "unit test description " + i;

      CollectionRequestServerModel collectionRequestServerModel =
          new CollectionRequestServerModel(name, description);

      CollectionServerModel actual =
          collectionService.save(workspaceId, collectionRequestServerModel);

      // assert it was created correctly
      assertCollectionExists(workspaceId, actual.getId(), name, description);

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
      assertThat(ids).contains(actual.getId());
    }
  }

  @Test
  void listCollectionsByWorkspace() throws Exception {
    WorkspaceId workspaceIdOne = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId workspaceIdTwo = WorkspaceId.of(UUID.randomUUID());

    var testSize = 3;

    // insert {testSize} collections into both workspace one and workspace two
    List<UUID> collectionIdsOne =
        IntStream.range(0, testSize)
            .mapToObj(i -> insertCollection(workspaceIdOne, "name" + i, "description").getId())
            .toList();
    List<UUID> collectionIdsTwo =
        IntStream.range(0, testSize)
            .mapToObj(i -> insertCollection(workspaceIdTwo, "name" + i, "description").getId())
            .toList();

    // assert the inserts are different
    assertNotEquals(collectionIdsOne, collectionIdsTwo);

    // mock read permission on both workspaces
    stubReadWorkspacePermission(workspaceIdOne).thenReturn(true);
    stubReadWorkspacePermission(workspaceIdTwo).thenReturn(true);

    // list collections for workspace one
    MvcResult mvcResult =
        mockMvc
            .perform(get("/collections/v1/{workspaceId}", workspaceIdOne))
            .andExpect(status().isOk())
            .andReturn();

    List<CollectionServerModel> collectionServerModelList =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), new TypeReference<>() {});

    List<UUID> actual =
        collectionServerModelList.stream().map(CollectionServerModel::getId).toList();
    assertEquals(collectionIdsOne, actual);
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

  @Test
  void updateCollection() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();

    CollectionServerModel actual =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), CollectionServerModel.class);

    assertEquals(collectionId.id(), actual.getId());
    assertEquals(updateName, actual.getName());
    assertEquals(updateDescription, actual.getDescription());

    assertCollectionExists(workspaceId, collectionId, updateName, updateDescription);
  }

  @Test
  void updateCollectionReadonlyPermission() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // read-only permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
        mvcResult.getResolvedException().getMessage());

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void updateCollectionNoPermission() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // no permission
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
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

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void updateNonexistentCollection() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // ensure write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andReturn();

    assertInstanceOf(MissingObjectException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection does not exist or you do not have permission to see it",
        mvcResult.getResolvedException().getMessage());

    // collection should not exist
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void updateNonexistentCollectionReadonlyPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // read-only permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);
    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
        mvcResult.getResolvedException().getMessage());

    // collection should not exist
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void updateNonexistentCollectionNoPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());

    // no permission
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);
    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
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

    // collection should not exist
    assertCollectionDoesNotExist(collectionId);
  }

  @Test
  void updateCollectionWithMismatchedWorkspaceId() throws Exception {
    WorkspaceId workspaceIdOne = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId workspaceIdTwo = WorkspaceId.of(UUID.randomUUID());
    // create a collection as setup, in workspace one
    CollectionServerModel collectionServerModel = insertCollection(workspaceIdOne);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure write permission to workspace two
    stubWriteWorkspacePermission(workspaceIdTwo).thenReturn(true);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update the collection in workspace two (it really lives in workspace one)
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceIdTwo, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andReturn();

    assertInstanceOf(MissingObjectException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection does not exist or you do not have permission to see it",
        mvcResult.getResolvedException().getMessage());

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceIdOne,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void updateCollectionWithMismatchedWorkspaceIdReadonlyPermission() throws Exception {
    WorkspaceId workspaceIdOne = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId workspaceIdTwo = WorkspaceId.of(UUID.randomUUID());
    // create a collection as setup, in workspace one
    CollectionServerModel collectionServerModel = insertCollection(workspaceIdOne);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // read-only permission to workspace two
    stubReadWorkspacePermission(workspaceIdTwo).thenReturn(true);
    stubWriteWorkspacePermission(workspaceIdTwo).thenReturn(false);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);
    // attempt to update the collection in workspace two (it really lives in workspace one)
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceIdTwo, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
        mvcResult.getResolvedException().getMessage());

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceIdOne,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void updateCollectionWithMismatchedWorkspaceIdNoPermission() throws Exception {
    WorkspaceId workspaceIdOne = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId workspaceIdTwo = WorkspaceId.of(UUID.randomUUID());
    // create a collection as setup, in workspace one
    CollectionServerModel collectionServerModel = insertCollection(workspaceIdOne);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // no permission to workspace two
    stubReadWorkspacePermission(workspaceIdTwo).thenReturn(false);
    stubWriteWorkspacePermission(workspaceIdTwo).thenReturn(false);

    // generate an update request
    String updateName = "updated-name";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);
    // attempt to update the collection in workspace two (it really lives in workspace one)
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceIdTwo, collectionId)
                    .content(toJson(updateRequest))
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

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceIdOne,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void updateCollectionNameConflict() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    // create two collections with names "one" and "two"
    CollectionServerModel collectionServerModelOne =
        insertCollection(workspaceId, "one", "desc one");
    CollectionServerModel collectionServerModelTwo =
        insertCollection(workspaceId, "two", "desc two");

    // ensure write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // generate an update request, trying to rename collection "two" to "one"
    String updateName = "one";
    String updateDescription = "this should fail";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update collection "two"
    MvcResult mvcResult =
        mockMvc
            .perform(
                put(
                        "/collections/v1/{workspaceId}/{collectionId}",
                        workspaceId,
                        collectionServerModelTwo.getId())
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isConflict())
            .andReturn();

    assertInstanceOf(ConflictException.class, mvcResult.getResolvedException());
    assertEquals(
        "Collection with this name already exists in this workspace",
        mvcResult.getResolvedException().getMessage());

    // both collections should exist untouched
    assertCollectionExists(
        workspaceId,
        CollectionId.of(collectionServerModelOne.getId()),
        collectionServerModelOne.getName(),
        collectionServerModelOne.getDescription());
    assertCollectionExists(
        workspaceId,
        CollectionId.of(collectionServerModelTwo.getId()),
        collectionServerModelTwo.getName(),
        collectionServerModelTwo.getDescription());
  }

  @Test
  void updateCollectionNameConflictReadonlyPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    // create two collections with names "one" and "two"
    CollectionServerModel collectionServerModelOne =
        insertCollection(workspaceId, "one", "desc one");
    CollectionServerModel collectionServerModelTwo =
        insertCollection(workspaceId, "two", "desc two");

    // read-only permission
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // generate an update request, trying to rename collection "two" to "one"
    String updateName = "one";
    String updateDescription = "this should fail";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update collection "two"
    MvcResult mvcResult =
        mockMvc
            .perform(
                put(
                        "/collections/v1/{workspaceId}/{collectionId}",
                        workspaceId,
                        collectionServerModelTwo.getId())
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
    assertEquals(
        "403 FORBIDDEN \"You are not allowed to write this workspace\"",
        mvcResult.getResolvedException().getMessage());

    // both collections should exist untouched
    assertCollectionExists(
        workspaceId,
        CollectionId.of(collectionServerModelOne.getId()),
        collectionServerModelOne.getName(),
        collectionServerModelOne.getDescription());
    assertCollectionExists(
        workspaceId,
        CollectionId.of(collectionServerModelTwo.getId()),
        collectionServerModelTwo.getName(),
        collectionServerModelTwo.getDescription());
  }

  @Test
  void updateCollectionNameConflictNoPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    // create two collections with names "one" and "two"
    CollectionServerModel collectionServerModelOne =
        insertCollection(workspaceId, "one", "desc one");
    CollectionServerModel collectionServerModelTwo =
        insertCollection(workspaceId, "two", "desc two");

    // no permission
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    // generate an update request, trying to rename collection "two" to "one"
    String updateName = "one";
    String updateDescription = "this should fail";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update collection "two"
    MvcResult mvcResult =
        mockMvc
            .perform(
                put(
                        "/collections/v1/{workspaceId}/{collectionId}",
                        workspaceId,
                        collectionServerModelTwo.getId())
                    .content(toJson(updateRequest))
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

    // both collections should exist untouched
    assertCollectionExists(
        workspaceId,
        CollectionId.of(collectionServerModelOne.getId()),
        collectionServerModelOne.getName(),
        collectionServerModelOne.getDescription());
    assertCollectionExists(
        workspaceId,
        CollectionId.of(collectionServerModelTwo.getId()),
        collectionServerModelTwo.getName(),
        collectionServerModelTwo.getDescription());
  }

  @Test
  void updateCollectionInvalidName() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // generate an update request
    String updateName = "hey! this is an invalid name!";
    String updateDescription = "updated description";
    CollectionRequestServerModel updateRequest =
        new CollectionRequestServerModel(updateName, updateDescription);

    // attempt to update the collection
    MvcResult mvcResult =
        mockMvc
            .perform(
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content(toJson(updateRequest))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isBadRequest())
            .andReturn();

    // verify error message
    assertInstanceOf(MethodArgumentNotValidException.class, mvcResult.getResolvedException());

    ErrorResponse errorResponse =
        objectMapper.readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    assertEquals("name: must match \"[a-zA-Z0-9-_]{1,128}\"", errorResponse.getMessage());

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  @Test
  void updateCollectionNoChanges() throws Exception {
    // create a collection as setup
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collectionServerModel = insertCollection(workspaceId);
    CollectionId collectionId = CollectionId.of(collectionServerModel.getId());

    // ensure write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // attempt to update the collection, but don't specify any changes
    mockMvc
        .perform(
            put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                .content(toJson(collectionServerModel))
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andReturn();

    // collection should not be updated (collection should exist with original name/description)
    assertCollectionExists(
        workspaceId,
        collectionId,
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  // ==================== test utilities

  private CollectionServerModel insertCollection(WorkspaceId workspaceId) {
    return insertCollection(workspaceId, "collection-name", "collection description");
  }

  private CollectionServerModel insertCollection(
      WorkspaceId workspaceId, String name, String description) {
    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

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
    CollectionServerModel actual =
        collectionService.save(workspaceId, collectionRequestServerModel);
    assertCollectionExists(workspaceId, actual.getId(), name, description);

    // reset write permission to original state
    stubWriteWorkspacePermission(workspaceId).thenReturn(originalWritePermission);

    return actual;
  }

  private void assertCollectionExists(
      WorkspaceId workspaceId, UUID collectionUuid, String name, String description) {
    assertCollectionExists(workspaceId, CollectionId.of(collectionUuid), name, description);
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

  private void assertCollectionDoesNotExist(String name) {
    assertThrows(
        Exception.class,
        () ->
            namedTemplate.queryForObject(
                "select id, workspace_id, name, description from sys_wds.collection where name = :name",
                new MapSqlParameterSource(Map.of("name", name)),
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
