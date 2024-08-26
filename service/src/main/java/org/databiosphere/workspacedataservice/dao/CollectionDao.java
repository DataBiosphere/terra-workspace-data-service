package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

@Deprecated(forRemoval = true, since = "v0.18.0")
public interface CollectionDao {
  boolean collectionSchemaExists(CollectionId collectionId);

  void createSchema(CollectionId collectionId);

  void dropSchema(CollectionId collectionId);

  void alterSchema(CollectionId oldCollectionId, CollectionId newCollectionId);

  WorkspaceId getWorkspaceId(CollectionId collectionId);
}
