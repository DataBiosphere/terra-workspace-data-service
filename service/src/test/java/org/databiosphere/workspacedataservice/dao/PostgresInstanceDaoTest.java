package org.databiosphere.workspacedataservice.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=80000000-4000-4000-4000-11000000000",
    })
class PostgresInstanceDaoTest {

  @Autowired InstanceDao instanceDao;
  @Autowired NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  @Value("${twds.instance.workspace-id}")
  UUID workspaceId;

  // is this test set up correctly?
  @Test
  void isPostgresInstanceDao() {
    assertInstanceOf(
        PostgresInstanceDao.class,
        instanceDao,
        "Tests in this file expect InstanceDao to be a PostgresInstanceDao; if this isn't true,"
            + "other test cases could fail.");
  }

  // when creating an instance whose id is different from the containing workspace's id,
  // do we populate all db columns correctly?
  @Test
  void insertPopulatesAllColumns() {
    UUID instanceId = UUID.randomUUID();
    instanceDao.createSchema(instanceId);

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
    UUID instanceId = workspaceId;
    instanceDao.createSchema(instanceId);

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
