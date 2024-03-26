package org.databiosphere.workspacedataservice.sam;

import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

/**
 * Interface for SamDao, allowing various dao implementations. Currently, the only implementation is
 * HttpSamDao.
 */
public interface SamDao {
  // TODO(jladieu): get the token injected during creation and eliminate it from the arg list
  String getUserId(BearerToken token);

  String getUserEmail(BearerToken token);

  /** Gets the up/down system status of Sam. */
  Boolean getSystemStatusOk();

  /** Gets the System Status of Sam. */
  SystemStatus getSystemStatus();

  /** Gets a pet token for the user. * */
  String getPetToken();

  /** Add reader policy to workspace. */
  void addMemberPolicy(
      String resourceTypeName, WorkspaceId workspaceId, UUID snapshotId, String readerRole);
}
