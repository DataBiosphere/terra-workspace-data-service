package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface CollectionDao {
  boolean collectionSchemaExists(UUID collectionId);

  List<UUID> listCollectionSchemas();

  void createSchema(UUID collectionId);

  void dropSchema(UUID collectionId);

  void alterSchema(UUID sourceWorkspaceId, UUID workspaceId);

  WorkspaceId getWorkspaceId(CollectionId collectionId);
}
