package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.util.UUID;

public interface BackupDao {
    Job<BackupResponse> getBackupStatus(UUID trackingId);

    boolean backupExists(UUID trackingId);

    void createBackupEntry(UUID trackingId, BackupRequest backupRequest);

    void updateBackupStatus(UUID trackingId, JobStatus status);

    void terminateBackupToError(UUID trackingId, String error);

    void updateFilename(UUID trackingId, String filename);
}
