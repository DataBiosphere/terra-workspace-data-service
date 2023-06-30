package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.BackupSchema;

import java.util.UUID;

public interface BackupDao {
    BackupSchema getBackupStatus(UUID trackingId);

    String getBackupRequestStatus(UUID sourceWorkspaceId, UUID destinationWorkspaceId);

    boolean backupExists(UUID trackingId);

    void createBackupEntry(UUID trackingId);

    void updateBackupStatus(UUID trackingId, BackupSchema.BackupState status);

    void updateBackupRequestStatus(UUID sourceWorkspaceId, BackupSchema.BackupState status);

    void createBackupRequestsEntry(UUID sourceWorkspaceId, UUID destinationWorkspaceId) ;

    void updateFilename(UUID trackingId, String filename);
}