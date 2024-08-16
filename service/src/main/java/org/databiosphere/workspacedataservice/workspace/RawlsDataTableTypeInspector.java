package org.databiosphere.workspacedataservice.workspace;

import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class RawlsDataTableTypeInspector implements DataTableTypeInspector {

  private final RawlsClient rawlsClient;

  public RawlsDataTableTypeInspector(RawlsClient rawlsClient) {
    this.rawlsClient = rawlsClient;
  }

  /**
   * Determine the type of data tables that the given workspace uses, by making a REST request to
   * Rawls. If Rawls says this is an MC workspace, this method will say the workspace is powered by
   * WDS. Else, this method will say the workspace is powered by Rawls Entity Service.
   *
   * @param workspaceId the workspace to check
   * @return whether the workspace should use WDS-powered or Rawls-powered data tables
   */
  @Override
  public WorkspaceDataTableType getWorkspaceDataTableType(WorkspaceId workspaceId) {
    RawlsWorkspaceDetails details = rawlsClient.getWorkspaceDetails(workspaceId.id());
    if (RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType.MC.equals(
        details.workspace().workspaceType())) {
      return WorkspaceDataTableType.WDS;
    }
    return WorkspaceDataTableType.RAWLS;
  }
}
