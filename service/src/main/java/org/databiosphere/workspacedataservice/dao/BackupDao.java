package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;

import java.util.UUID;

public interface BackupDao {
    BackupSchema getBackupStatus(UUID trackingId);

    boolean backupExists(UUID trackingId);

    void createBackupEntry(UUID trackingId, UUID sourceWorkspaceId);

    void updateBackupStatus(UUID trackingId, String status);

    boolean backupExistsForGivenSource(UUID sourceWorkspaceId);

    void updateFilename(UUID trackingId, String filename);
}
