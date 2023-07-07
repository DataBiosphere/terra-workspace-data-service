package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of CloneDao that is in-memory instead of requiring Postgres
 */
public class MockCloneDao implements CloneDao {
    private final Set<Job<CloneEntry>> clone = ConcurrentHashMap.newKeySet();
    public MockCloneDao() {
        super();
    }
    @Override
    public boolean cloneExistsForWorkspace(UUID workspaceId)  {
        return clone.stream().anyMatch(entry -> entry.getResult().sourceWorkspaceId.equals(workspaceId));
    }

    @Override
    public void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId) {
        Timestamp now = Timestamp.from(Instant.now());
        var cloneEntry = new CloneEntry(sourceWorkspaceId, CloneStatus.BACKUPQUEUED);
        Job<CloneEntry> jobEntry = new Job<>(trackingId, JobStatus.QUEUED, "", now.toLocalDateTime(), now.toLocalDateTime(), cloneEntry);
        clone.add(jobEntry);
    }

    @Override
    public void updateCloneEntryStatus(UUID trackingId, CloneStatus status) {
        try {
            var cloneEntry = clone.stream().filter(entry -> entry.getJobId().equals(trackingId)).findFirst().orElse(null);
            clone.remove(cloneEntry);
            cloneEntry.getResult().status = status;
            cloneEntry.setStatus(JobStatus.SUCCEEDED);
            clone.add(cloneEntry);
        }
        catch(Exception e) {
            var cloneEntry = clone.stream().filter(entry -> entry.getJobId().equals(trackingId)).findFirst().orElse(null);
            clone.remove(cloneEntry);
            cloneEntry.setStatus(JobStatus.ERROR);
            clone.add(cloneEntry);
        }
    }

    @Override
    @WriteTransaction
    public void terminateBackupToError(UUID trackingId, String error) {
        var cloneEntry = clone.stream().filter(entry -> entry.getJobId().equals(trackingId)).findFirst().orElse(null);
        clone.remove(cloneEntry);
        cloneEntry.setErrorMessage(error);
        clone.add(cloneEntry);
        updateCloneEntryStatus(trackingId, CloneStatus.BACKUPERROR);
    }

    class CloneEntry implements JobResult {
        public UUID sourceWorkspaceId;
        public CloneStatus status;

        public CloneEntry(UUID workspaceId, CloneStatus jobStatus) {
            sourceWorkspaceId = workspaceId;
            status = jobStatus;
        }
    }
}