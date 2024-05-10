package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import static java.util.Collections.emptyList;

import bio.terra.workspace.model.WorkspaceDescription;
import bio.terra.workspace.model.WsmPolicyInput;
import java.util.List;
import java.util.Optional;
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
    WorkspaceDescription workspace = wsmDao.getWorkspace(workspaceId.id());
    List<WsmPolicyInput> workspacePolicies =
        Optional.ofNullable(workspace.getPolicies()).orElse(emptyList());
    return workspacePolicies.stream().anyMatch(PolicyUtils::isProtectedDataPolicy);
  }
}
