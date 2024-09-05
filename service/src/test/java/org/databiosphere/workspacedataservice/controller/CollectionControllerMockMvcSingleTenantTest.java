package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
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

/**
 * Tests for create-collection behavior in a WDS running in single-tenant mode. These tests are
 * invalid for a multi-tenant WDS.
 */
@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@TestPropertySource(
    properties = {"twds.instance.workspace-id=45f90f59-f83d-453f-961a-480ec740df9f"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CollectionControllerMockMvcSingleTenantTest extends MockMvcTestBase {

  @Autowired private ObjectMapper objectMapper;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TwdsProperties twdsProperties;

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
  // create a collection inside the single-tenant workspace
  void createCollectionInSingleTenantWorkspace() throws Exception {
    WorkspaceId workspaceId = twdsProperties.workspaceId();
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
    assertNotEquals(
        collectionId.id(),
        actualResponse.getId(),
        "Collection id in create request should be ignored");
    assertEquals(name, actualResponse.getName(), "incorrect collection name");
    assertEquals(description, actualResponse.getDescription(), "incorrect collection description");

    // the collection should exist in the db
    assertCollectionExists(workspaceId, CollectionId.of(actualResponse.getId()), name, description);
  }

  @Test
  // attempt to create a collection outside the single-tenant workspace
  void createCollectionOutsideSingleTenantWorkspace() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID()); // not twdsProperties.workspaceId()
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    String name = "test-name";
    String description = "unit test description";

    CollectionServerModel collectionServerModel = new CollectionServerModel(name, description);
    collectionServerModel.id(collectionId.id());

    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // calling the API should result in 400 Bad Request
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/collections/v1/{workspaceId}", workspaceId)
                    .content(toJson(collectionServerModel))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isBadRequest())
            .andReturn();

    // verify error message
    assertInstanceOf(ValidationException.class, mvcResult.getResolvedException());
    assertEquals(
        "Cannot create collection in this workspace.",
        mvcResult.getResolvedException().getMessage());

    // collection should not be created
    assertCollectionDoesNotExist(collectionId);
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
