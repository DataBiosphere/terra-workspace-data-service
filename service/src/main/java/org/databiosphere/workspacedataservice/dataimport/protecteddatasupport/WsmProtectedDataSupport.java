package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import bio.terra.workspace.model.WorkspaceDescription;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.policy.PolicyUtils;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.stereotype.Component;

@Component
@DataPlane
public class WsmProtectedDataSupport implements ProtectedDataSupport {
  private final WorkspaceManagerDao wsmDao;

  public WsmProtectedDataSupport(WorkspaceManagerDao wsmDao) {
    this.wsmDao = wsmDao;
  }

  public boolean workspaceSupportsProtectedDataPolicy(WorkspaceId workspaceId) {
    WorkspaceDescription workspace = wsmDao.getWorkspace(workspaceId);
    return PolicyUtils.containsProtectedDataPolicy(workspace.getPolicies());
  }
}
