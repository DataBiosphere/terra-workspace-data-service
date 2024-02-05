package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface InstanceDao {
  boolean instanceSchemaExists(UUID instanceId);

  List<UUID> listInstanceSchemas();

  void createSchema(UUID instanceId);

  void dropSchema(UUID instanceId);

  void alterSchema(UUID sourceWorkspaceId, UUID workspaceId);

  WorkspaceId getWorkspaceId(InstanceId instanceId);
}
