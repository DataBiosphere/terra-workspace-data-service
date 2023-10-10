package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.shared.model.job.JobType.SYNCCLONE;

import bio.terra.common.db.WriteTransaction;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.job.EmptyJobInput;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

/** Mock implementation of CloneDao that is in-memory instead of requiring Postgres */
public class MockCloneDao implements CloneDao {
  private final Set<Job<EmptyJobInput, CloneResponse>> clone = ConcurrentHashMap.newKeySet();

  public MockCloneDao() {
    super();
  }

  @Override
  public boolean cloneExistsForWorkspace(UUID workspaceId) {
    return clone.stream()
        .anyMatch(entry -> entry.getResult().sourceWorkspaceId().equals(workspaceId));
  }

  @Override
  public void createCloneEntry(UUID trackingId, UUID sourceWorkspaceId) {
    LocalDateTime now = Timestamp.from(Instant.now()).toLocalDateTime();
    var cloneEntry = new CloneResponse(sourceWorkspaceId, CloneStatus.BACKUPQUEUED);
    Job<EmptyJobInput, CloneResponse> jobEntry =
        new Job<>(
            trackingId, SYNCCLONE, JobStatus.QUEUED, "", now, now, new EmptyJobInput(), cloneEntry);
    clone.add(jobEntry);
  }

  @Override
  public void updateCloneEntryStatus(UUID trackingId, CloneStatus status) {
    var cloneEntry = findCloneEntry(trackingId);
    if (cloneEntry != null) {
      clone.remove(cloneEntry);
      cloneEntry.setResult(new CloneResponse(cloneEntry.getResult().sourceWorkspaceId(), status));
      cloneEntry.setStatus(JobStatus.SUCCEEDED);
      clone.add(cloneEntry);
    }
  }

  @Override
  @WriteTransaction
  public void terminateCloneToError(UUID trackingId, String error, CloneTable table) {
    var cloneEntry = findCloneEntry(trackingId);
    if (cloneEntry != null) {
      clone.remove(cloneEntry);
      cloneEntry.setErrorMessage(error);
      clone.add(cloneEntry);
      updateCloneEntryStatus(
          trackingId,
          table.equals(CloneTable.BACKUP) ? CloneStatus.BACKUPERROR : CloneStatus.RESTOREERROR);
    }
  }

  @Override
  public Job<EmptyJobInput, CloneResponse> getCloneStatus() {
    return clone.stream().max(Comparator.comparing(Job::getUpdated)).orElse(null);
  }

  private Job<EmptyJobInput, CloneResponse> findCloneEntry(UUID trackingId) {
    return clone.stream()
        .filter(entry -> entry.getJobId().equals(trackingId))
        .findFirst()
        .orElse(null);
  }
}
