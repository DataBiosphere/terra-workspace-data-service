package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

public interface BackupRestoreDao<T extends JobResult> {
  Job<JobInput, T> getStatus(UUID trackingId);

  void createEntry(UUID trackingId, BackupRestoreRequest BackupRestoreRequest);

  void updateStatus(UUID trackingId, JobStatus status);

  void terminateToError(UUID trackingId, String error);

  void updateFilename(UUID trackingId, String filename);
}
