package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;


import java.util.UUID;

public interface BackupRestoreDao {
    Job<BackupRestoreResponse> getStatus(UUID trackingId, Boolean isBackup);
    void createEntry(UUID trackingId, BackupRestoreRequest BackupRestoreRequest, Boolean isBackup);
    void updateStatus(UUID trackingId, JobStatus status, Boolean isBackup);
    void terminateToError(UUID trackingId, String error, Boolean isBackup);
    void updateFilename(UUID trackingId, String filename, Boolean isBackup);
}
