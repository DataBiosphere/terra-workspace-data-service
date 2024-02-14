package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresCollectionDao implements CollectionDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  private UUID workspaceId;

  /*
  PostgresCollectionDao is used to interact with sys_wds instance table in postgres.
  NOTE: sys_wds.instance will be renamed to sys_wds.collection in an upcoming PR for AJ-1592
  This table tracks activity such as collection creation and deletion, as well as returning existing collections.
  This class will help add entries to the table, check if entries already exist and update them as necessary.
   */
  public PostgresCollectionDao(
      NamedParameterJdbcTemplate namedTemplate, TwdsProperties twdsProperties) {
    this.namedTemplate = namedTemplate;
    // if we have a valid workspaceId, save it to a local var now
    if (twdsProperties.getCollection() != null
        && twdsProperties.getCollection().getWorkspaceUuid() != null) {
      this.workspaceId = twdsProperties.getCollection().getWorkspaceUuid();
    }
  }

  @Override
  public boolean collectionSchemaExists(UUID collectionId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.instance WHERE id = :collectionId)",
            new MapSqlParameterSource("collectionId", collectionId),
            Boolean.class));
  }

  @Override
  public List<UUID> listCollectionSchemas() {
    return namedTemplate
        .getJdbcTemplate()
        .queryForList("select id from sys_wds.instance order by id", UUID.class);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void createSchema(UUID collectionId) {
    // insert to instance table
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
        .update("delete from sys_wds.instance where id = ?", collectionId);
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
    insertCollectionRow(newSchemaId, /* ignoreConflict= */ true);
  }

  @Override
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    UUID workspaceUuid =
        namedTemplate.queryForObject(
            "select workspace_id from sys_wds.instance where id = :collectionId",
            new MapSqlParameterSource("collectionId", collectionId.id()),
            UUID.class);
    return WorkspaceId.of(workspaceUuid);
  }

  private void insertCollectionRow(UUID collectionId, boolean ignoreConflict) {
    // if workspaceId as configured by the $WORKSPACE_ID is null, use
    // instanceId instead
    UUID nonNullWorkspaceId = Objects.requireNonNullElse(workspaceId, collectionId);

    // auto-generate the name for this instance
    String name = collectionId.toString();
    if (collectionId.equals(nonNullWorkspaceId)) {
      name = "default";
    }

    MapSqlParameterSource params = new MapSqlParameterSource("id", collectionId);
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
