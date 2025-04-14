package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import java.util.List;
import org.databiosphere.workspacedataservice.policy.PolicyUtils;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Component;

@Component
public class RawlsProtectedDataSupport implements ProtectedDataSupport {
  private final RawlsClient rawlsClient;

  public RawlsProtectedDataSupport(RawlsClient rawlsClient) {
    this.rawlsClient = rawlsClient;
  }

  // TODO AJ-1957: move into WorkspaceService?
  public boolean workspaceSupportsProtectedDataPolicy(WorkspaceId workspaceId) {
    RawlsWorkspaceDetails workspaceDetails = rawlsClient.getWorkspaceDetails(workspaceId.id());
    WorkspaceType workspaceType = workspaceDetails.workspace().workspaceType();
    return switch (workspaceType) {
      case MC -> PolicyUtils.containsProtectedDataPolicy(workspaceDetails.policies());
      case RAWLS -> workspaceDetails.workspace().bucketName().startsWith("fc-secure-");
    };
  }

  public void addAuthDomainGroupsToWorkspace(
      WorkspaceId workspaceId, List<String> authDomainGroups) {
    RawlsWorkspaceDetails workspaceDetails = rawlsClient.getWorkspaceDetails(workspaceId.id());
    rawlsClient.addAuthDomainGroups(
        workspaceDetails.workspace().namespace(),
        workspaceDetails.workspace().name(),
        authDomainGroups);
  }
}
