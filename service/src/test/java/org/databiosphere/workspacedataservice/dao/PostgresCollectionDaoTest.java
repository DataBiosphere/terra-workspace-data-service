package org.databiosphere.workspacedataservice.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.Map;
import java.util.UUID;
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
class PostgresCollectionDaoTest {

  @Autowired CollectionDao collectionDao;
  @Autowired NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  @Value("${twds.instance.workspace-id}")
  UUID workspaceId;

  // clean up all instances before each test to ensure tests start from a clean slate
  @BeforeEach
  void beforeEach() {
    List<UUID> allInstances = collectionDao.listCollectionSchemas();
    allInstances.forEach(instanceId -> collectionDao.dropSchema(instanceId));
  }

  // clean up all instances after all tests to ensure tests in other files start from a clean slate
  @AfterAll
  void afterAll() {
    List<UUID> allInstances = collectionDao.listCollectionSchemas();
    allInstances.forEach(instanceId -> collectionDao.dropSchema(instanceId));
  }

  // is this test set up correctly?
  @Test
  void isPostgresInstanceDao() {
    assertInstanceOf(
        PostgresCollectionDao.class,
        collectionDao,
        "Tests in this file expect InstanceDao to be a PostgresInstanceDao; if this isn't true,"
            + "other test cases could fail.");
  }

  // when creating an instance whose id is different from the containing workspace's id,
  // do we populate all db columns correctly?
  @Test
  void insertPopulatesAllColumns() {

    List<UUID> allInstances = collectionDao.listCollectionSchemas();
    assertEquals(0, allInstances.size());

    UUID instanceId = UUID.randomUUID();
    collectionDao.createSchema(instanceId);

    Map<String, Object> rowMap =
        namedParameterJdbcTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.instance where id = :id",
            new MapSqlParameterSource("id", instanceId));

    assertEquals(instanceId, rowMap.get("id"));
    assertEquals(workspaceId, rowMap.get("workspace_id"));
    assertEquals(instanceId.toString(), rowMap.get("name"));
    assertEquals(instanceId.toString(), rowMap.get("description"));
  }

  // when creating an instance whose id is the same as the containing workspace's id,
  // do we populate all db columns correctly?
  @Test
  void defaultPopulatesAllColumns() {

    List<UUID> allInstances = collectionDao.listCollectionSchemas();
    assertEquals(0, allInstances.size());

    UUID instanceId = workspaceId;

    collectionDao.createSchema(instanceId);

    Map<String, Object> rowMap =
        namedParameterJdbcTemplate.queryForMap(
            "select id, workspace_id, name, description from sys_wds.instance where id = :id",
            new MapSqlParameterSource("id", instanceId));

    assertEquals(instanceId, rowMap.get("id"));
    assertEquals(workspaceId, rowMap.get("workspace_id"));
    assertEquals("default", rowMap.get("name"));
    assertEquals("default", rowMap.get("description"));
  }
}
