package org.databiosphere.workspacedataservice.sourcewds;


import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.BackupJob;
import org.databiosphere.workspacedata.model.BackupRequest;

import java.util.UUID;


public class WorkspaceDataServiceDao {
  private final WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;

  private String workspaceDataServiceUrl;

  public WorkspaceDataServiceDao(WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory) {
    this.workspaceDataServiceClientFactory = workspaceDataServiceClientFactory;
  }

  public void setWorkspaceDataServiceUrl(String endpointUrl) {
    this.workspaceDataServiceUrl = endpointUrl;
  }
  /**
   * Triggers a backup in source workspace data service.
   */
  public BackupJob triggerBackup(String token, UUID requesterWorkspaceId) {
    try {
      var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token, workspaceDataServiceUrl);
      BackupRequest body = new BackupRequest();
      body.setRequestingWorkspaceId(requesterWorkspaceId);
      return backupClient.createBackup(body,"v0.2");
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }

  /**
   * Checks status of a backup in source workspace data service.
   */
  public BackupJob checkBackupStatus(String token, UUID trackingId) {
    var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token, workspaceDataServiceUrl);
    try {
      return backupClient.getBackupStatus("v0.2", trackingId);
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }
}