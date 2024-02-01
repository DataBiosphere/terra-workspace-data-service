package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.model.WorkspaceId;

public interface InstanceDao {
  boolean instanceSchemaExists(UUID instanceId);

  List<UUID> listInstanceSchemas();

  void createSchema(UUID instanceId);

  void dropSchema(UUID instanceId);

  void alterSchema(UUID sourceWorkspaceId, UUID workspaceId);

  CollectionServerModel getCollection(UUID collectionId);

  CollectionServerModel getCollection(WorkspaceId workspaceId, UUID collectionId);

  List<CollectionServerModel> getCollections(WorkspaceId workspaceId);
}
