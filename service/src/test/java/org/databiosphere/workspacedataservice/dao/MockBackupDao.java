package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of BackupRestoreDao that is in-memory instead of requiring Postgres
 */
public class MockBackupDao <T extends JobResult> implements BackupRestoreDao<T> {

    // backing "database" for this mock
    private final Set<Job<T> > entries = ConcurrentHashMap.newKeySet();

    public MockBackupDao() {
        super();
    }

    @Override
    public Job<T> getStatus(UUID trackingId) {
        return entries.stream().filter(backupInList -> backupInList.getJobId() == trackingId).findFirst().orElse(null);
    }

    @Override
    public void createEntry(UUID trackingId, BackupRestoreRequest request) {
        Timestamp now = Timestamp.from(Instant.now());
        var metadata = new BackupResponse("", request.requestingWorkspaceId(), request.description());
        Job<T> backup = new Job<T>(trackingId, JobStatus.QUEUED, "", now.toLocalDateTime(), now.toLocalDateTime(), metadata);
        entries.add(backup);
    }

    @Override
    public void updateStatus(UUID trackingId, JobStatus status) {
        var backup = getStatus(trackingId);
        entries.remove(backup);
        backup.setStatus(status);
        entries.add(backup);
    }

    @Override
    public void updateFilename(UUID trackingId, String filename) {
        var backup = getStatus(trackingId);
        entries.remove(backup);
        BackupResponse response = new BackupResponse(filename, null, "Backup successfully completed.");
        backup.setResult(response);
        entries.add(backup);
    }

    @Override
    public void terminateToError(UUID trackingId, String error) {
        var backup = getStatus(trackingId);
        entries.remove(backup);
        backup.setStatus(JobStatus.ERROR);
        backup.setErrorMessage(error);
        entries.add(backup);
    }
}
