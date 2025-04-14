package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface ProtectedDataSupport {
  boolean workspaceSupportsProtectedDataPolicy(WorkspaceId workspaceId);

  void addAuthDomainGroupsToWorkspace(WorkspaceId workspaceId, List<String> authDomainGroups);
}
