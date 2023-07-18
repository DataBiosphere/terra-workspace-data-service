package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
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
public class MockBackupRestoreDao<T extends JobResult> implements BackupRestoreDao<T> {

    // backing "database" for this mock
    private final Set<Job<T>> entries = ConcurrentHashMap.newKeySet();

    private final CloneTable table;

    public MockBackupRestoreDao(CloneTable table) {
        this.table = table;
    }

    @Override
    public Job<T> getStatus(UUID trackingId) {
        return entries.stream().filter(backupInList -> backupInList.getJobId() == trackingId).findFirst().orElse(null);
    }

    @Override
    public void createEntry(UUID trackingId, BackupRestoreRequest request) {
        Timestamp now = Timestamp.from(Instant.now());
        if(table.equals(CloneTable.BACKUP)) {
            var metadata = new BackupResponse("", request.requestingWorkspaceId(), request.description());
            Job<BackupResponse> backup = new Job<>(trackingId, JobStatus.QUEUED, "", now.toLocalDateTime(), now.toLocalDateTime(), metadata);
            entries.add((Job<T>) backup);
        } else {
            var metadata = new RestoreResponse(request.requestingWorkspaceId(), request.description());
            Job<RestoreResponse> restore = new Job<>(trackingId, JobStatus.QUEUED, "", now.toLocalDateTime(), now.toLocalDateTime(), metadata);
            entries.add((Job<T>) restore);
        }

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
        backup.setResult((T) response);
        entries.add(backup);
    }

    @Override
    public void terminateToError(UUID trackingId, String error) {
        var entry = getStatus(trackingId);
        entries.remove(entry);
        entry.setStatus(JobStatus.ERROR);
        entry.setErrorMessage(error);
        entries.add(entry);
    }
}
