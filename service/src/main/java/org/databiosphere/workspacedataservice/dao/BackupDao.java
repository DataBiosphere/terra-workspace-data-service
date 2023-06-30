package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;
import org.databiosphere.workspacedataservice.shared.model.BackupRequest;

import java.util.UUID;

public interface BackupDao {
    BackupSchema getBackupStatus(UUID trackingId);

    boolean backupExists(UUID trackingId);

    void createBackupEntry(UUID trackingId, BackupRequest backupRequest);

    void updateBackupStatus(UUID trackingId, BackupSchema.BackupState status);

    void saveBackupError(UUID trackingId, String error);

    void updateFilename(UUID trackingId, String filename);
}