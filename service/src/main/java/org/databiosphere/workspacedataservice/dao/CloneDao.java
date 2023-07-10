package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.CloneStatus;

import java.util.UUID;

public interface CloneDao {
    void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId);
    void updateCloneEntryStatus(UUID trackingId, CloneStatus status);
    boolean cloneExistsForWorkspace(UUID sourceWorkspaceId);
    void terminateCloneToError(UUID trackingId, String error);
}
