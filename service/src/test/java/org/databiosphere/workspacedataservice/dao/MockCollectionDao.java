package org.databiosphere.workspacedataservice.dao;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.postgresql.util.ServerErrorMessage;

/** Mock implementation of CollectionDao that is in-memory instead of requiring Postgres */
public class MockCollectionDao implements CollectionDao {

  // backing "database" for this mock
  private final Set<CollectionId> collections = ConcurrentHashMap.newKeySet();
  private final WorkspaceId workspaceId;

  public MockCollectionDao(WorkspaceId workspaceId) {
    this.workspaceId = workspaceId;
  }

  @Override
  public boolean collectionSchemaExists(CollectionId collectionId) {
    return collections.contains(collectionId);
  }

  @Override
  public void alterSchema(CollectionId oldCollectionId, CollectionId newCollectionId) {
    if (!collections.contains(oldCollectionId)) {
      ServerErrorMessage sqlMsg =
          new ServerErrorMessage("ERROR: schema \"" + oldCollectionId + "\" does not exist");
      SQLException ex = new org.postgresql.util.PSQLException(sqlMsg);
      String sql = "alter schema \"" + oldCollectionId + "\" rename to \"" + newCollectionId + "\"";
      throw new org.springframework.jdbc.BadSqlGrammarException("StatementCallback", sql, ex);
    }
    collections.remove(oldCollectionId);
    collections.add(newCollectionId);
  }

  @Override
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    return workspaceId;
  }

  // convenience for unit tests: removes all collections
  public void clearAllCollections() {
    Set<CollectionId> toRemove = Set.copyOf(collections);
    collections.removeAll(toRemove);
  }
}
