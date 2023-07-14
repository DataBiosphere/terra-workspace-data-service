package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of BackupRestoreDao that is in-memory instead of requiring Postgres
 */
public class MockBackupRestoreDao implements BackupRestoreDao {

    // backing "database" for this mock
    private final Set<Job<BackupRestoreResponse> > db = ConcurrentHashMap.newKeySet();

    public MockBackupRestoreDao() {
        super();
    }

    @Override
    public Job<BackupRestoreResponse> getStatus(UUID trackingId, Boolean isBackup) {
        return db.stream().filter(backupInList -> backupInList.getJobId() == trackingId).findFirst().orElse(null);
    }

    @Override
    public void createEntry(UUID trackingId, BackupRestoreRequest request, Boolean isBackup) {
        Timestamp now = Timestamp.from(Instant.now());
        var metadata = new BackupRestoreResponse("", request.requestingWorkspaceId(), request.description());
        Job<BackupRestoreResponse> backup = new Job<>(trackingId, JobStatus.QUEUED, "", now.toLocalDateTime(), now.toLocalDateTime(), metadata);
        db.add(backup);
    }

    @Override
    public void updateStatus(UUID trackingId, JobStatus status, Boolean isBackup) {
        var backup = getStatus(trackingId, isBackup);
        db.remove(backup);
        backup.setStatus(status);
        db.add(backup);
    }

    @Override
    public void updateFilename(UUID trackingId, String filename, Boolean isBackup) {
        var backup = getStatus(trackingId, isBackup);
        db.remove(backup);
        BackupRestoreResponse response = new BackupRestoreResponse(filename, null, "Backup successfully completed.");
        backup.setResult(response);
        db.add(backup);
    }

    @Override
    public void terminateToError(UUID trackingId, String error, Boolean isBackup) {
        var backup = getStatus(trackingId, isBackup);
        db.remove(backup);
        backup.setStatus(JobStatus.ERROR);
        backup.setErrorMessage(error);
        db.add(backup);
    }
}
