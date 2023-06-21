package org.databiosphere.workspacedataservice.sourcewds;


import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.BackupResponse;
import org.databiosphere.workspacedata.model.BackupTrackingResponse;

import java.util.UUID;


public class WorkspaceDataServiceDao {
  private final WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;

  public WorkspaceDataServiceDao(WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory, String workspaceId) {
    this.workspaceDataServiceClientFactory = workspaceDataServiceClientFactory;
  }

  /**
   * Triggers a backup in source workspace data service.
   */
  public BackupTrackingResponse triggerBackup(String token, UUID requesterWorkspaceId) {
    var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token);
    try {
      var response = backupClient.createBackup("v0.2", requesterWorkspaceId);
      return response;
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }

  public BackupResponse checkBackupStatus(String token, UUID trackingId) {
    var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token);
    try {
      var response = backupClient.getBackupStatus(trackingId);
      return response;
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }
}