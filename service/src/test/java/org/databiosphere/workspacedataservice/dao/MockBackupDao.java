package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.BackupRequest;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of BackupDao that is in-memory instead of requiring Postgres
 */
public class MockBackupDao implements BackupDao {

    // backing "database" for this mock
    private final Set<Job<BackupResponse> > backups = ConcurrentHashMap.newKeySet();

    public MockBackupDao() {
        super();
    }


    @Override
    public boolean backupExists(UUID trackingId) {
        var data = backups.stream().filter(entry -> entry.getJobId().equals(trackingId));
        return data == null ? false : true ;
    }

    @Override
    public boolean backupExistsForWorkspace(UUID workspaceId)  {
        var data = backups.stream().filter(entry -> entry.getResult().requester().equals(workspaceId));
        return data == null ? false : true ;
    }

    @Override
    public Job<BackupResponse> getBackupStatus(UUID trackingId) {
        return backups.stream().filter(backupInList -> backupInList.getJobId() == trackingId).findFirst().orElse(null);
    }

    @Override
    public void createBackupEntry(UUID trackingId, BackupRequest request) {
        Job<BackupResponse> backup = new Job<>(trackingId, JobStatus.QUEUED,"",null, null, null);
        backups.add(backup);
    }

    @Override
    public void updateBackupStatus(UUID trackingId, JobStatus status) {
        var backup = getBackupStatus(trackingId);
        backups.remove(backup);
        backup.setStatus(status);
        backups.add(backup);
    }

    @Override
    public void updateFilename(UUID trackingId, String filename) {
        var backup = getBackupStatus(trackingId);
        backups.remove(backup);
        BackupResponse response = new BackupResponse(filename, null, "backup success");
        backup.setResult(response);
        backups.add(backup);
    }

    @Override
    public void terminateBackupToError(UUID trackingId, String error) {

    }
}
