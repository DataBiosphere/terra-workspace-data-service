package org.databiosphere.workspacedataservice.sourcewds;

import org.databiosphere.workspacedata.api.CloningApi;

public interface WorkspaceDataServiceClientFactory {
  CloningApi getBackupClient(String token, String wdsUrl);
}
