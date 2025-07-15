package org.databiosphere.workspacedataservice.sam;

public interface SamAuthorizationDao {
  /** Sam resource type name for Workspaces */
  String RESOURCE_NAME_WORKSPACE = "workspace";

  /** Sam action name for write permission */
  String ACTION_WRITE = "write";

  /** Sam action name for read permission */
  String ACTION_READ = "read";

  /** Sam policy name for read permission */
  String READER_POLICY_NAME = "reader";

  /* Resource type to specify when adding Sam member policies */
  String RESOURCE_NAME_TDR_SNAPSHOT = "datasnapshot";

  /** All roles to add reader policy */
  String[] WORKSPACE_ROLES = {"reader", "writer", "owner", "project-owner"};

  /**
   * Check if the current user has permission to read the workspace resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasReadWorkspacePermission();

  /**
   * Check if the current user has permission to write to a workspace resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasWriteWorkspacePermission();
}
