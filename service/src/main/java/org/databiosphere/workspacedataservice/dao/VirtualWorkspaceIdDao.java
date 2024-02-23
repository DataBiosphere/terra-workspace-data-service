package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Repository;

/**
 * VirtualWorkspaceIdDao encapsulates the assumption that collection and workspace ids are the same.
 * This is true in the control plane as long as Rawls continues to manage data tables, since
 * "collection" is a WDS concept and does not exist in Rawls.
 */
@ControlPlane
@Repository
public class VirtualWorkspaceIdDao implements WorkspaceIdDao {
  @NotNull
  @Override
  public WorkspaceId getWorkspaceId(CollectionId collectionId) {
    return WorkspaceId.of(collectionId.id());
  }
}
