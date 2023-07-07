package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.CloneStatus;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of CloneDao that is in-memory instead of requiring Postgres
 */
public class MockCloneDao implements CloneDao {
    private final Set<CloneEntry> clone = ConcurrentHashMap.newKeySet();
    public MockCloneDao() {
        super();
    }
    @Override
    public boolean cloneExistsForWorkspace(UUID workspaceId)  {
        return clone.stream().anyMatch(entry -> entry.sourceWorkspaceId.equals(workspaceId));
    }

    @Override
    public void createCloneEntry(UUID sourceWorkspaceId) {
        clone.add(new CloneEntry(sourceWorkspaceId, CloneStatus.BACKUPQUEUED));
    }

    @Override
    public void updateCloneEntryStatus(UUID sourceWorkspaceId, CloneStatus status) {
        var cloneEntry = clone.stream().filter(entry -> entry.sourceWorkspaceId.equals(sourceWorkspaceId)).findFirst().orElse(null);
        clone.remove(cloneEntry);
        cloneEntry.status = status;
        clone.add(cloneEntry);
    }

    class CloneEntry {
        public UUID sourceWorkspaceId;
        public CloneStatus status;

        public CloneEntry(UUID workspaceId, CloneStatus jobStatus) {
            sourceWorkspaceId = workspaceId;
            status = jobStatus;
        }
    }
}