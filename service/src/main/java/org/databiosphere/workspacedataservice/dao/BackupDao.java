package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;

import java.util.UUID;

public interface BackupDao {
    BackupSchema getBackupStatus(UUID trackingId);

    boolean backupExists(UUID trackingId);

    void createBackupEntry(UUID trackingId);

    void updateBackupStatus(UUID trackingId, BackupSchema.BackupState status);

    void updateFilename(UUID trackingId, String filename);
}