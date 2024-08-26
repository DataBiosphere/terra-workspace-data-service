package org.databiosphere.workspacedataservice.dao;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresCollectionDaoTest extends TestBase {

  @Autowired CollectionDao collectionDao;
  @Autowired CollectionService collectionService;
  @Autowired NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  @Autowired TwdsProperties twdsProperties;
  @Autowired @SingleTenant WorkspaceId workspaceId;

  // clean up all collections before each test to ensure tests start from a clean slate
  @BeforeEach
  void beforeEach() {
    TestUtils.cleanAllCollections(collectionService, namedParameterJdbcTemplate);
  }

  // clean up all collections after all tests to ensure tests in other files start from a clean
  // slate
  @AfterAll
  void afterAll() {
    TestUtils.cleanAllCollections(collectionService, namedParameterJdbcTemplate);
  }

  // is this test set up correctly?
  @Test
  void isPostgresCollectionDao() {
    assertInstanceOf(
        PostgresCollectionDao.class,
        collectionDao,
        "Tests in this file expect CollectionDao to be a PostgresCollectionDao; if this isn't true,"
            + "other test cases could fail.");
  }

  // when creating a collection whose id is different from the containing workspace's id,
  // do we populate all db columns correctly?
  @Test
  void insertPopulatesAllColumns() {
    assertEquals(0, collectionService.list(twdsProperties.workspaceId()).size());

    CollectionId collectionId = CollectionId.of(randomUUID());
    UUID collectionUuid = collectionId.id();

    collectionDao.createSchema(collectionId);

    Map<String, Object> rowMap =
        namedParameterJdbcTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.collection where id = :id",
            new MapSqlParameterSource("id", collectionUuid));

    assertEquals(collectionUuid, rowMap.get("id"));
    assertEquals(workspaceId.id(), rowMap.get("workspace_id"));
    assertEquals(collectionId.toString(), rowMap.get("name"));
    assertEquals(collectionId.toString(), rowMap.get("description"));
  }

  // when creating a collection whose id is the same as the containing workspace's id,
  // do we populate all db columns correctly?
  @Test
  void defaultPopulatesAllColumns() {
    assertEquals(0, collectionService.list(twdsProperties.workspaceId()).size());

    UUID collectionUuid = workspaceId.id();
    CollectionId collectionId = CollectionId.of(collectionUuid);

    collectionDao.createSchema(collectionId);

    Map<String, Object> rowMap =
        namedParameterJdbcTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.collection where id = :id",
            new MapSqlParameterSource("id", collectionUuid));

    assertEquals(collectionUuid, rowMap.get("id"));
    assertEquals(workspaceId.id(), rowMap.get("workspace_id"));
    assertEquals("default", rowMap.get("name"));
    assertEquals("default", rowMap.get("description"));
  }
}
