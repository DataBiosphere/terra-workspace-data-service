package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.CloneStatus;

import java.util.UUID;

public interface CloneDao {
    void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId);
    void updateCloneEntryStatus(UUID sourceWorkspaceId, CloneStatus status);
    boolean cloneExistsForWorkspace(UUID workspaceId);
}
