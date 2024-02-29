package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface SamAuthorizationDao {
  // TODO(jladieu): get the WorkspaceId injected during creation and remove it from all arg lists
  // TODO(jladieu): collapse overloads after the extra params are gone

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
  boolean hasReadWorkspacePermission(WorkspaceId workspaceId);

  /**
   * Check if the current user has permission to write to a workspace resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasWriteWorkspacePermission();

  boolean hasWriteWorkspacePermission(WorkspaceId workspaceId);
}
