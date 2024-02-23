package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.config.InstanceProperties.SingleTenant;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.jetbrains.annotations.NotNull;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * PostgresCollectionDao is used to interact with sys_wds collection table in postgres. This table
 * tracks activity such as collection creation and deletion, as well as returning existing
 * collections. This class will help add entries to the table, check if entries already exist and
 * update them as necessary.
 */
@DataPlane
@Repository
public class PostgresCollectionDao implements CollectionDao, WorkspaceIdDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  private final WorkspaceId workspaceId;

  public PostgresCollectionDao(
      NamedParameterJdbcTemplate namedTemplate, @SingleTenant WorkspaceId workspaceId) {
    this.namedTemplate = namedTemplate;
    this.workspaceId = workspaceId;
  }

  @Override
  public boolean collectionSchemaExists(UUID collectionId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.collection WHERE id = :collectionId)",
            new MapSqlParameterSource("collectionId", collectionId),
            Boolean.class));
  }

  @Override
  public List<UUID> listCollectionSchemas() {
    return namedTemplate
        .getJdbcTemplate()
        .queryForList("select id from sys_wds.collection order by id", UUID.class);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void createSchema(UUID collectionId) {
    // insert to collection table
    insertCollectionRow(collectionId, /* ignoreConflict= */ false);
    // create the postgres schema
    namedTemplate.getJdbcTemplate().update("create schema " + quote(collectionId.toString()));
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void dropSchema(UUID collectionId) {
    namedTemplate
        .getJdbcTemplate()
        .update("drop schema " + quote(collectionId.toString()) + " cascade");
    namedTemplate
        .getJdbcTemplate()
        .update("delete from sys_wds.collection where id = ?", collectionId);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void alterSchema(UUID oldSchemaId, UUID newSchemaId) {
    // rename the pg schema from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter schema "
                + quote(oldSchemaId.toString())
                + " rename to "
                + quote(newSchemaId.toString()));
    // rename any rows in sys_wds.collection from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "update sys_wds.collection set id = ?, workspace_id = ? where id = ?",
            newSchemaId,
            workspaceId.id(),
            oldSchemaId);
    // ensure new exists in sys_wds.collection. When this alterSchema() method is called after
    // restoring from a pg_dump,
    // the oldSchema doesn't exist, so is not renamed in the previous statement.
    insertCollectionRow(newSchemaId, /* ignoreConflict= */ true);
  }

  @NotNull
  @Override
  public WorkspaceId getWorkspaceId(CollectionId collectionId) throws MissingObjectException {
    try {
      UUID workspaceUuid =
          namedTemplate.queryForObject(
              "select workspace_id from sys_wds.collection where id = :collectionId",
              new MapSqlParameterSource("collectionId", collectionId.id()),
              UUID.class);
      return WorkspaceId.of(workspaceUuid);
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Missing collection with id %s".formatted(collectionId));
    }
  }

  private void insertCollectionRow(UUID collectionId, boolean ignoreConflict) {
    // auto-generate the name for this collection
    String name = collectionId.toString();
    if (collectionId.equals(workspaceId.id())) {
      name = "default";
    }

    MapSqlParameterSource params = new MapSqlParameterSource("id", collectionId);
    params.addValue("workspace_id", workspaceId.id());
    params.addValue("name", name);
    params.addValue("description", name);

    String onConflictClause = "";
    if (ignoreConflict) {
      onConflictClause = " on conflict(id) do nothing";
    }

    namedTemplate.update(
        "insert into sys_wds.collection(id, workspace_id, name, description)"
            + " values (:id, :workspace_id, :name, :description)"
            + onConflictClause,
        params);
  }
}
