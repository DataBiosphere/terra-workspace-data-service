package org.databiosphere.workspacedataservice.sam;

public interface SamAuthorizationDao {
  /** Sam resource type name for Workspaces */
  public static final String RESOURCE_NAME_WORKSPACE = "workspace";

  /** Sam action name for write permission */
  public static final String ACTION_WRITE = "write";

  /** Sam action name for read permission */
  public static final String ACTION_READ = "read";

  /** Sam policy name for read permission */
  public static final String READER_POLICY_NAME = "reader";

  /* Resource type to specify when adding Sam member policies */
  public static final String RESOURCE_NAME_TDR_SNAPSHOT = "datasnapshot";

  /** All roles to add reader policy */
  public static final String[] WORKSPACE_ROLES = {"reader", "writer", "owner", "project-owner"};

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
