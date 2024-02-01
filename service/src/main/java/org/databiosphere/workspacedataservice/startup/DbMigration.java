package org.databiosphere.workspacedataservice.startup;

import bio.terra.common.db.WriteTransaction;
import jakarta.annotation.PostConstruct;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DbMigration {

  @Value("${twds.instance.workspace-id}")
  private UUID workspaceId;

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public DbMigration(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  // TODO davidan: can this be done purely in liquibase somehow?
  @PostConstruct
  @WriteTransaction
  void migrateInstances() {
    var params = new MapSqlParameterSource("workspaceId", workspaceId);
    namedParameterJdbcTemplate.update(
        "update sys_wds.instance set workspace_id = :workspaceId where workspace_id is null",
        params);
    namedParameterJdbcTemplate.update(
        "update sys_wds.instance set name = 'default' where name is null and id = :workspaceId",
        params);
    namedParameterJdbcTemplate.update(
        "update sys_wds.instance set name = left(id::text,8) where name is null;", params);

    // after these updates, it is safe to set the table to be non-null
  }
}
