package org.databiosphere.workspacedataservice.workspace;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class WdsDataTableTypeInspector implements DataTableTypeInspector {

  /**
   * Always returns WDS. This implementation of DataTableTypeInspector is used in the data plane,
   * which has no Rawls client. By nature, if this WDS is running in the data plane, its data tables
   * are powered by WDS.
   *
   * @param workspaceId the workspace to check
   * @return whether the workspace should use WDS-powered or Rawls-powered data tables
   */
  @Override
  public WorkspaceDataTableType getWorkspaceDataTableType(WorkspaceId workspaceId) {
    return WorkspaceDataTableType.WDS;
  }
}
