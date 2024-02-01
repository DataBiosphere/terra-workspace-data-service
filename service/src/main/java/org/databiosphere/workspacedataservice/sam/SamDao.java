package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.model.WorkspaceId;

/**
 * Interface for SamDao, allowing various dao implementations. Currently, the only implementation is
 * HttpSamDao.
 */
public interface SamDao {

  /** Sam resource type name for Workspaces */
  String RESOURCE_NAME_WORKSPACE = "workspace";

  /** Sam action name for write permission */
  String ACTION_READ = "read";

  /** Sam action name for write permission */
  String ACTION_WRITE = "write";

  /** Sam action name for delete permission */
  String ACTION_DELETE = "delete";

  /**
   * Check if the current user has permission to create a "wds-instance" resource in Sam
   *
   * @return true if the user has permission
   */
  boolean hasCreateInstancePermission();

  boolean hasCreateInstancePermission(String token);

  // boolean hasCreateInstancePermission(WorkspaceId workspaceId, String token);

  /**
   * Check if the current user has permission to delete a "wds-instance" resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasDeleteInstancePermission();

  boolean hasDeleteInstancePermission(String token);

  // boolean hasDeleteInstancePermission(InstanceId instanceIdId, String token);

  /**
   * Check if the current user has permission to write to a "wds-instance" resource from Sam
   *
   * @return true if the user has permission
   */
  boolean hasWriteInstancePermission();

  boolean hasWriteInstancePermission(String token);

  boolean hasReadWorkspacePermission(WorkspaceId workspaceId, String token);

  boolean hasWriteWorkspacePermission(WorkspaceId workspaceId, String token);

  String getUserId(String token);

  /** Gets the up/down system status of Sam. */
  Boolean getSystemStatusOk();

  /** Gets the System Status of Sam. */
  SystemStatus getSystemStatus();

  /** Gets a pet token for the user * */
  String getPetToken();
}
