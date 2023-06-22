package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of InstanceDao that is in-memory instead of requiring Postgres
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
    public String getBackupStatus(UUID trackingId) {
        return "";
    }

    @Override
    public void createBackupEntry(UUID trackingId, UUID sourceWorkspaceId) {
        BackupSchema backup = new BackupSchema(trackingId, sourceWorkspaceId);
        backups.add(backup);
    }

    @Override
    public void updateBackupStatus(UUID trackingId, String status) {
        BackupSchema backup = backups.
    }

    @Override
    public void updateFilename(UUID trackingId, String filename) {

    }

    @Override
    public boolean backupExistsForGivenSource(UUID sourceWorkspaceId) {
        return backups.contains(sourceWorkspaceId);
    }
}
