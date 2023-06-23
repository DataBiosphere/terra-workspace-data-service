package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of BackupDao that is in-memory instead of requiring Postgres
 */
public class MockBackupDao implements BackupDao {

    // backing "database" for this mock
    private final Set<BackupSchema> backups = ConcurrentHashMap.newKeySet();

    public MockBackupDao() {
        super();
    }


    @Override
    public boolean backupExists(UUID trackingId) {
        return backups.contains(trackingId);
    }

    @Override
    public BackupSchema getBackupStatus(UUID trackingId) {
        return backups.stream().filter(backupInList -> backupInList.getId() == trackingId).findFirst().orElse(null);
    }

    @Override
    public String getBackupRequestStatus(UUID sourceWorkspaceId, UUID destinationWorkspaceId) {
        return null;
    }

    @Override
    public void createBackupEntry(UUID trackingId) {
        BackupSchema backup = new BackupSchema(trackingId);
        backups.add(backup);
    }

    @Override
    public void createBackupRequestsEntry(UUID trackingId, UUID sourceWorkspaceId) {
        // currently not used in tests but needs to be here due to the interface
    }

    @Override
    public void updateBackupStatus(UUID trackingId, String status) {
        BackupSchema backup = getBackupStatus(trackingId);
        backups.remove(backup);
        backup.setState(BackupSchema.BackupState.valueOf(status));
        backups.add(backup);
    }

    @Override
    public void updateBackupRequestStatus(UUID sourceWorkspaceId, BackupSchema.BackupState status){
        // currently not used in tests but needs to be here due to the interface
    }

    @Override
    public void updateFilename(UUID trackingId, String filename) {
        BackupSchema backup = getBackupStatus(trackingId);
        backups.remove(backup);
        backup.setFileName(filename);
        backups.add(backup);
    }
}
