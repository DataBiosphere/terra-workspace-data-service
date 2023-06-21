package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;

public interface BackupDao {
    String getBackupStatus(UUID trackingId);

    boolean backupExists(UUID trackingId);

    void createBackupEntry(UUID trackingId, UUID sourceWorkspaceId);

    void updateBackupStatus(UUID trackingId, String status);

    boolean backupExistsForGivenSource(UUID sourceWorkspaceId);

    void updateFilename(UUID trackingId, String filename);
}
