package org.databiosphere.workspacedataservice.workspace;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

/** Inspects a workspace and decides what type of data table (WDS, RAWLS) this workspace uses. */
public interface DataTableTypeInspector {
  /**
   * Determine the type of data tables that the given workspace uses.
   *
   * @param workspaceId the workspace to check
   * @return whether the workspace should use WDS-powered or Rawls-powered data tables
   */
  WorkspaceDataTableType getWorkspaceDataTableType(WorkspaceId workspaceId);
}
