package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.lang.NonNull;

public interface WorkspaceIdDao {
  /**
   * Returns the {@link WorkspaceId} for the given collectionId.
   *
   * @throws MissingObjectException if the given collection does not exist
   */
  @NonNull
  WorkspaceId getWorkspaceId(CollectionId collectionId) throws MissingObjectException;
}
