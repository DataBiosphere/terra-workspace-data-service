package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

/**
 * Interface for SamDao, allowing various dao implementations. Currently, the only implementation is
 * HttpSamDao.
 */
public interface SamDao {

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

  boolean hasCreateCollectionPermission(BearerToken token);

  /**
   * Check if the current user has permission to delete a collection associated with a Sam workspace
   * resource
   *
   * @return true if the user has permission
   */
  boolean hasDeleteCollectionPermission();

  boolean hasDeleteCollectionPermission(BearerToken token);

  /**
   * Check if the current user has permission to read the workspace resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasReadWorkspacePermission(WorkspaceId workspaceId);

  boolean hasReadWorkspacePermission(WorkspaceId workspaceId, BearerToken token);

  /**
   * Check if the current user has permission to write to a workspace resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasWriteWorkspacePermission();

  boolean hasWriteWorkspacePermission(WorkspaceId workspaceId);

  String getUserId(BearerToken token);

  /** Gets the up/down system status of Sam. */
  Boolean getSystemStatusOk();

  /** Gets the System Status of Sam. */
  SystemStatus getSystemStatus();

  /** Gets a pet token for the user * */
  String getPetToken();
}
