package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface ProtectedDataSupport {
  boolean workspaceSupportsProtectedDataPolicy(WorkspaceId workspaceId);
}
