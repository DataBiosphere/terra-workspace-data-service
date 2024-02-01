package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.model.WorkspaceId;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresInstanceDao implements InstanceDao {

  private final NamedParameterJdbcTemplate namedTemplate;

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
    namedTemplate
        .getJdbcTemplate()
        .update("insert into sys_wds.instance(id) values (?)", instanceId);
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
        .update("update sys_wds.instance set id = ? where id = ?", newSchemaId, oldSchemaId);
    // ensure new exists in sys_wds.instance. When this alterSchema() method is called after
    // restoring from a pg_dump,
    // the oldSchema doesn't exist, so is not renamed in the previous statement.
    namedTemplate
        .getJdbcTemplate()
        .update(
            "insert into sys_wds.instance(id) values (?) on conflict(id) do nothing", newSchemaId);
  }

  @Override
  public CollectionServerModel getCollection(UUID collectionId) {
    return namedTemplate.queryForObject(
        "select id, workspace_id, name, description from sys_wds.instance where id = :id",
        new MapSqlParameterSource("id", collectionId),
        new CollectionServerModelRowMapper());
  }

  @Override
  public CollectionServerModel getCollection(WorkspaceId workspaceId, UUID collectionId) {
    return namedTemplate.queryForObject(
        "select id, workspace_id, name, description from sys_wds.instance where id = :id and workspace_id = :workspaceId",
        new MapSqlParameterSource(Map.of("id", collectionId, "workspaceId", workspaceId.id())),
        new CollectionServerModelRowMapper());
  }

  public List<CollectionServerModel> getCollections(WorkspaceId workspaceId) {
    return namedTemplate.query(
        "select id, workspace_id, name, description from sys_wds.instance where workspace_id = :workspaceId order by name desc",
        new MapSqlParameterSource("workspaceId", workspaceId.id()),
        new CollectionServerModelRowMapper());
  }

  private static class CollectionServerModelRowMapper implements RowMapper<CollectionServerModel> {
    @Override
    public CollectionServerModel mapRow(ResultSet rs, int rowNum) throws SQLException {
      UUID id = UUID.fromString(rs.getString("id"));
      UUID workspaceId = UUID.fromString(rs.getString("workspace_id"));
      String name = rs.getString("name");
      String description = rs.getString("description");

      return new CollectionServerModel(id, workspaceId, name, description);
    }
  }
}
