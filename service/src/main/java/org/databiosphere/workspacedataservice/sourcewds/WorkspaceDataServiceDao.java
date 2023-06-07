package org.databiosphere.workspacedataservice.sourcewds;


import org.databiosphere.workspacedata.client.ApiException;

public class WorkspaceDataServiceDao {
  private final WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;
  private final String workspaceId;

  public WorkspaceDataServiceDao(WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory, String workspaceId) {
    this.workspaceDataServiceClientFactory = workspaceDataServiceClientFactory;
    this.workspaceId = workspaceId;
  }

  /**
   * Triggers a backup in source workspace data service.
   */
  public void triggerBackup(String token) {
    var backupClient = this.workspaceDataServiceClientFactory.getBackupClient(token);
    try {
      var response = backupClient.createBackup("vo.2");
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }
}