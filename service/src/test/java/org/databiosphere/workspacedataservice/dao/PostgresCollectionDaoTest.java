package org.databiosphere.workspacedataservice.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=80000000-4000-4000-4000-120000000000",
    })
class PostgresCollectionDaoTest extends TestBase {

  @Autowired CollectionDao collectionDao;
  @Autowired WorkspaceIdDao workspaceIdDao;
  @Autowired NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  @Value("${twds.instance.workspace-id}")
  UUID workspaceId;

  // clean up all collections before each test to ensure tests start from a clean slate
  @BeforeEach
  void beforeEach() {
    List<UUID> allCollections = collectionDao.listCollectionSchemas();
    allCollections.forEach(collectionId -> collectionDao.dropSchema(collectionId));
  }

  // clean up all collections after all tests to ensure tests in other files start from a clean
  // slate
  @AfterAll
  void afterAll() {
    List<UUID> allCollections = collectionDao.listCollectionSchemas();
    allCollections.forEach(collectionId -> collectionDao.dropSchema(collectionId));
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

    List<UUID> allCollections = collectionDao.listCollectionSchemas();
    assertEquals(0, allCollections.size());

    UUID collectionId = UUID.randomUUID();
    collectionDao.createSchema(collectionId);

    Map<String, Object> rowMap =
        namedParameterJdbcTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.collection where id = :id",
            new MapSqlParameterSource("id", collectionId));

    assertEquals(collectionId, rowMap.get("id"));
    assertEquals(workspaceId, rowMap.get("workspace_id"));
    assertEquals(collectionId.toString(), rowMap.get("name"));
    assertEquals(collectionId.toString(), rowMap.get("description"));
  }

  // when creating a collection whose id is the same as the containing workspace's id,
  // do we populate all db columns correctly?
  @Test
  void defaultPopulatesAllColumns() {

    List<UUID> allCollections = collectionDao.listCollectionSchemas();
    assertEquals(0, allCollections.size());

    UUID collectionId = workspaceId;

    collectionDao.createSchema(collectionId);

    Map<String, Object> rowMap =
        namedParameterJdbcTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.collection where id = :id",
            new MapSqlParameterSource("id", collectionId));

    assertEquals(collectionId, rowMap.get("id"));
    assertEquals(workspaceId, rowMap.get("workspace_id"));
    assertEquals("default", rowMap.get("name"));
    assertEquals("default", rowMap.get("description"));
  }

  @Test
  void getWorkspaceId_returnsWhenPresent() {
    UUID collectionId = UUID.randomUUID();
    collectionDao.createSchema(collectionId);
    assertEquals(
        WorkspaceId.of(workspaceId), workspaceIdDao.getWorkspaceId(CollectionId.of(collectionId)));
  }

  @Test
  void getWorkspaceId_raisesWhenMissing() {
    var nonexistentCollectionId = UUID.randomUUID();
    var thrown =
        assertThrows(
            MissingObjectException.class,
            () -> {
              workspaceIdDao.getWorkspaceId(CollectionId.of(nonexistentCollectionId));
            });

    assertThat(thrown).hasMessageContaining(nonexistentCollectionId.toString());
  }
}
