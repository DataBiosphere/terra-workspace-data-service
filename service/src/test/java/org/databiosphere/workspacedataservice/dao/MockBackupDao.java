package org.databiosphere.workspacedataservice.dao;

import org.postgresql.util.ServerErrorMessage;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock implementation of InstanceDao that is in-memory instead of requiring Postgres
 */
public class MockBackupDao implements BackupDao {

    // backing "database" for this mock
    private final Set<UUID> backups = ConcurrentHashMap.newKeySet();

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

    }

    @Override
    public void updateBackupStatus(UUID trackingId, String status) {

    }

    @Override
    public void updateFilename(UUID trackingId, String filename) {

    }

}
