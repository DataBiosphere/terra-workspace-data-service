package org.databiosphere.workspacedataservice.shared.model;

import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.time.LocalDateTime;
import java.util.UUID;

public class BackupJob extends Job<BackupResponse> {
    protected BackupJob(JobStatus status, UUID jobId, String errorMessage, LocalDateTime created, LocalDateTime updated, Exception exception, BackupResponse result) {
        super(jobId, status, errorMessage, created, updated, result);
    }
}
