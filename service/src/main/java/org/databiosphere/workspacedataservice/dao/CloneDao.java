package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;

public interface CloneDao {
  void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId);

  void updateCloneEntryStatus(UUID trackingId, CloneStatus status);

  boolean cloneExistsForWorkspace(UUID sourceWorkspaceId);

  Job<JobInput, CloneResponse> getCloneStatus();

  void terminateCloneToError(UUID trackingId, String error, CloneTable table);
}
