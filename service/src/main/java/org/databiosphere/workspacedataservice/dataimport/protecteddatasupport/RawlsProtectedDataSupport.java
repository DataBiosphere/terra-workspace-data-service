package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import static java.util.Collections.emptyList;

import bio.terra.workspace.model.WsmPolicyInput;
import java.util.List;
import java.util.Optional;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.policy.PolicyUtils;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Component;

@Component
@ControlPlane
public class RawlsProtectedDataSupport implements ProtectedDataSupport {
  private final RawlsClient rawlsClient;

  public RawlsProtectedDataSupport(RawlsClient rawlsClient) {
    this.rawlsClient = rawlsClient;
  }

  public boolean workspaceSupportsProtectedDataPolicy(WorkspaceId workspaceId) {
    RawlsWorkspaceDetails workspaceDetails = rawlsClient.getWorkspaceDetails(workspaceId.id());
    WorkspaceType workspaceType = workspaceDetails.workspace().workspaceType();
    return switch (workspaceType) {
      case MC -> {
        List<WsmPolicyInput> workspacePolicies =
            Optional.ofNullable(workspaceDetails.policies()).orElse(emptyList());
        yield workspacePolicies.stream().anyMatch(PolicyUtils::isProtectedDataPolicy);
      }
      case RAWLS -> {
        String bucketName = workspaceDetails.workspace().bucketName();
        yield bucketName.startsWith("fc-secure-");
      }
    };
  }
}
