package org.databiosphere.workspacedataservice.sam;

public interface SamAuthorizationDao {
  /** Sam resource type name for Workspaces */
  String RESOURCE_NAME_WORKSPACE = "workspace";

  /** Sam action name for write permission */
  String ACTION_WRITE = "write";

  /** Sam action name for delete permission */
  String ACTION_DELETE = "delete";

  /** Sam action name for read permission */
  String ACTION_READ = "read";

  /**
   * Check if the current user has permission to create a collection associated with a Sam workspace
   * resource
   *
   * @return true if the user has permission
   */
  boolean hasCreateCollectionPermission();

  /**
   * Check if the current user has permission to delete a collection associated with a Sam workspace
   * resource
   *
   * @return true if the user has permission
   */
  boolean hasDeleteCollectionPermission();

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
