package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

/**
 * PostgresCollectionDao is used to interact with sys_wds collection table in postgres. This table
 * tracks activity such as collection creation and deletion, as well as returning existing
 * collections. This class will help add entries to the table, check if entries already exist and
 * update them as necessary.
 */
@Repository
public class PostgresCollectionDao implements CollectionDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  @Nullable private UUID workspaceId;

  public PostgresCollectionDao(NamedParameterJdbcTemplate namedTemplate) {
    this.namedTemplate = namedTemplate;
  }

  @Autowired(required = false) // control plane won't have workspaceId
  void setWorkspaceId(@SingleTenant WorkspaceId workspaceId) {
    this.workspaceId = workspaceId.id();
  }

  @Override
  public boolean collectionSchemaExists(CollectionId collectionId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.collection WHERE id = :collectionId)",
            new MapSqlParameterSource("collectionId", collectionId.id()),
            Boolean.class));
  }

  @Override
  public List<CollectionId> listCollectionSchemas() {
    return namedTemplate
        .getJdbcTemplate()
        .queryForList("select id from sys_wds.collection order by id", UUID.class)
        .stream()
        .map(CollectionId::of)
        .toList();
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void createSchema(CollectionId collectionId) {
    // insert to collection table
    insertCollectionRow(collectionId, /* ignoreConflict= */ false);
    // create the postgres schema
    namedTemplate.getJdbcTemplate().update("create schema " + quote(collectionId.toString()));
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void dropSchema(CollectionId collectionId) {
    namedTemplate
        .getJdbcTemplate()
        .update("drop schema " + quote(collectionId.toString()) + " cascade");
    namedTemplate
        .getJdbcTemplate()
        .update("delete from sys_wds.collection where id = ?", collectionId.id());
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void alterSchema(CollectionId oldCollectionId, CollectionId newCollectionId) {
    if (workspaceId == null) {
      throw new UnsupportedOperationException(
          "$WORKSPACE_ID environment variable is required for alterSchema()");
    }
    // rename the pg schema from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter schema "
                + quote(oldCollectionId.toString())
                + " rename to "
                + quote(newCollectionId.toString()));
    // rename any rows in sys_wds.collection from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "update sys_wds.collection set id = ?, workspace_id = ? where id = ?",
            newCollectionId.id(),
            workspaceId,
            oldCollectionId.id());
    // ensure new exists in sys_wds.collection. When this alterSchema() method is called after
    // restoring from a pg_dump,
    // the oldSchema doesn't exist, so is not renamed in the previous statement.
    insertCollectionRow(newCollectionId, /* ignoreConflict= */ true);
  }

  @Override
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    UUID workspaceUuid =
        namedTemplate.queryForObject(
            "select workspace_id from sys_wds.collection where id = :collectionId",
            new MapSqlParameterSource("collectionId", collectionId.id()),
            UUID.class);
    return WorkspaceId.of(workspaceUuid);
  }

  private void insertCollectionRow(CollectionId collectionId, boolean ignoreConflict) {
    // if workspaceId as configured by the $WORKSPACE_ID is null, use
    // collectionId instead
    UUID nonNullWorkspaceUuid = Objects.requireNonNullElse(workspaceId, collectionId.id());

    // auto-generate the name for this collection
    String name = collectionId.toString();
    if (collectionId.id().equals(nonNullWorkspaceUuid)) {
      name = "default";
    }

    MapSqlParameterSource params = new MapSqlParameterSource("id", collectionId.id());
    params.addValue("workspace_id", nonNullWorkspaceUuid);
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
