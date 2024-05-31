package org.databiosphere.workspacedataservice.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.postgresql.util.ServerErrorMessage;

/** Mock implementation of CollectionDao that is in-memory instead of requiring Postgres */
public class MockCollectionDao implements CollectionDao {

  // backing "database" for this mock
  private final Set<UUID> collections = ConcurrentHashMap.newKeySet();
  private final WorkspaceId workspaceId;

  public MockCollectionDao(WorkspaceId workspaceId) {
    this.workspaceId = workspaceId;
  }

  @Override
  public boolean collectionSchemaExists(CollectionId collectionId) {
    return collections.contains(collectionId.id());
  }

  @Override
  public List<CollectionId> listCollectionSchemas() {
    return collections.stream().map(CollectionId::of).toList();
  }

  @Override
  public void createSchema(UUID collectionId) {
    if (collections.contains(collectionId)) {
      ServerErrorMessage sqlMsg =
          new ServerErrorMessage(
              "ERROR: schema \"" + collectionId.toString() + "\" already exists");
      SQLException ex = new org.postgresql.util.PSQLException(sqlMsg);
      String sql = "create schema \"" + collectionId + "\"";
      throw new org.springframework.jdbc.BadSqlGrammarException("StatementCallback", sql, ex);
    }
    collections.add(collectionId);
  }

  @Override
  public void dropSchema(UUID collectionId) {
    if (!collections.contains(collectionId)) {
      ServerErrorMessage sqlMsg =
          new ServerErrorMessage(
              "ERROR: schema \"" + collectionId.toString() + "\" does not exist");
      SQLException ex = new org.postgresql.util.PSQLException(sqlMsg);
      String sql = "drop schema \"" + collectionId + "\" cascade";
      throw new org.springframework.jdbc.BadSqlGrammarException("StatementCallback", sql, ex);
    }
    collections.remove(collectionId);
  }

  @Override
  public void alterSchema(UUID sourceWorkspaceId, UUID workspaceId) {
    if (!collections.contains(sourceWorkspaceId)) {
      ServerErrorMessage sqlMsg =
          new ServerErrorMessage(
              "ERROR: schema \"" + sourceWorkspaceId.toString() + "\" does not exist");
      SQLException ex = new org.postgresql.util.PSQLException(sqlMsg);
      String sql = "alter schema \"" + sourceWorkspaceId + "\" rename to \"" + workspaceId + "\"";
      throw new org.springframework.jdbc.BadSqlGrammarException("StatementCallback", sql, ex);
    }
    collections.remove(sourceWorkspaceId);
    collections.add(workspaceId);
  }

  @Override
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    return workspaceId;
  }

  // convenience for unit tests: removes all collections
  public void clearAllCollections() {
    Set<UUID> toRemove = Set.copyOf(collections);
    collections.removeAll(toRemove);
  }
}
