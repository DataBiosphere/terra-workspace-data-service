package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface CollectionDao {
  boolean collectionSchemaExists(CollectionId collectionId);

  List<CollectionId> listCollectionSchemas();

  void createSchema(CollectionId collectionId);

  void dropSchema(CollectionId collectionId);

  void alterSchema(CollectionId oldCollectionId, CollectionId newCollectionId);

  WorkspaceId getWorkspaceId(CollectionId collectionId);
}
