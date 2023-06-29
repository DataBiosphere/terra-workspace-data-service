package org.databiosphere.workspacedataservice.sourcewds;


import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.BackupResponse;
import org.databiosphere.workspacedata.model.BackupTrackingResponse;

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
  public BackupTrackingResponse triggerBackup(String token, UUID requesterWorkspaceId) {
    var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token, workspaceDataServiceUrl);
    try {
      return backupClient.createBackup("v0.2", requesterWorkspaceId);
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }

  /**
   * Checks status of a backup in source workspace data service.
   */
  public BackupResponse checkBackupStatus(String token, UUID trackingId) {
    var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token, workspaceDataServiceUrl);
    try {
      return backupClient.getBackupStatus(trackingId);
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }
}