package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.job.Job;

import java.util.UUID;

public interface CloneDao {
    void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId);
    void updateCloneEntryStatus(UUID trackingId, CloneStatus status);
    boolean cloneExistsForWorkspace(UUID sourceWorkspaceId);
    Job<CloneResponse> getCloneStatus();
    void terminateCloneToError(UUID trackingId, String error, CloneTable table);
}
