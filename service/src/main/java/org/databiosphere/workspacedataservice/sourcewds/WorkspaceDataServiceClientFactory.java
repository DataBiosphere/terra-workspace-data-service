package org.databiosphere.workspacedataservice.sourcewds;

import org.databiosphere.workspacedata.api.BackupApi;

public interface WorkspaceDataServiceClientFactory {
    BackupApi getBackupClient(String token);
}
