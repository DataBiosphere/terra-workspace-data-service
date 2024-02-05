package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresInstanceDao implements InstanceDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  @Value("${twds.instance.workspace-id}")
  private UUID workspaceId;

  /*
  PostgresInstanceDao is used to interact with sys_wds instance table in postgres.
  This table tracks activity such as instance creation and deletion, as well as returning existing instances.
  This class will help add entries to the table, check if entries already exist and update them as necessary.
   */
  public PostgresInstanceDao(NamedParameterJdbcTemplate namedTemplate) {
    this.namedTemplate = namedTemplate;
  }

  @Override
  public boolean instanceSchemaExists(UUID instanceId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.instance WHERE id = :instanceId)",
            new MapSqlParameterSource("instanceId", instanceId),
            Boolean.class));
  }

  @Override
  public List<UUID> listInstanceSchemas() {
    return namedTemplate
        .getJdbcTemplate()
        .queryForList("select id from sys_wds.instance order by id", UUID.class);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
  public void createSchema(UUID instanceId) {
    // insert to instance table
    insertInstanceRow(instanceId, false);
    // create the postgres schema
    namedTemplate.getJdbcTemplate().update("create schema " + quote(instanceId.toString()));
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
  public void dropSchema(UUID instanceId) {
    namedTemplate
        .getJdbcTemplate()
        .update("drop schema " + quote(instanceId.toString()) + " cascade");
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.instance where id = ?", instanceId);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
  public void alterSchema(UUID oldSchemaId, UUID newSchemaId) {
    // rename the pg schema from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter schema "
                + quote(oldSchemaId.toString())
                + " rename to "
                + quote(newSchemaId.toString()));
    // rename any rows in sys_wds.instance from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "update sys_wds.instance set id = ?, workspace_id = ? where id = ?",
            newSchemaId,
            workspaceId,
            oldSchemaId);
    // ensure new exists in sys_wds.instance. When this alterSchema() method is called after
    // restoring from a pg_dump,
    // the oldSchema doesn't exist, so is not renamed in the previous statement.
    insertInstanceRow(newSchemaId, true);
  }

  @Override
  public WorkspaceId getWorkspaceId(InstanceId instanceId) {
    UUID workspaceUuid =
        namedTemplate.queryForObject(
            "select workspace_id from sys_wds.instance where id = :instanceId",
            new MapSqlParameterSource("instanceId", instanceId.id()),
            UUID.class);
    return WorkspaceId.of(workspaceUuid);
  }

  private void insertInstanceRow(UUID instanceId, boolean ignoreConflict) {
    // if workspaceId as configured by the $WORKSPACE_ID is null, use
    // instanceId instead
    UUID nonNullWorkspaceId = Objects.requireNonNullElse(workspaceId, instanceId);

    // auto-generate the name for this instance
    String name = instanceId.toString();
    if (instanceId.equals(nonNullWorkspaceId)) {
      name = "default";
    }

    MapSqlParameterSource params = new MapSqlParameterSource("id", instanceId);
    params.addValue("workspace_id", nonNullWorkspaceId);
    params.addValue("name", name);
    params.addValue("description", name);

    String onConflictClause = "";
    if (ignoreConflict) {
      onConflictClause = " on conflict(id) do nothing";
    }

    namedTemplate.update(
        "insert into sys_wds.instance(id, workspace_id, name, description)"
            + " values (:id, :workspace_id, :name, :description)"
            + onConflictClause,
        params);
  }
}
