package org.databiosphere.workspacedataservice.sourcewds;

import java.util.UUID;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedata.model.BackupJob;
import org.databiosphere.workspacedata.model.BackupRestoreRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkspaceDataServiceDao {
  private final WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;

  private String workspaceDataServiceUrl;

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceDataServiceDao.class);

  public WorkspaceDataServiceDao(
      WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory) {
    this.workspaceDataServiceClientFactory = workspaceDataServiceClientFactory;
  }

  public void setWorkspaceDataServiceUrl(String endpointUrl) {
    this.workspaceDataServiceUrl = endpointUrl;
  }

  /** Triggers a backup in source workspace data service. */
  public BackupJob triggerBackup(String token, UUID requesterWorkspaceId) {
    try {
      var backupClient =
          this.workspaceDataServiceClientFactory.getBackupClient(token, workspaceDataServiceUrl);
      BackupRestoreRequest body = new BackupRestoreRequest();
      body.setRequestingWorkspaceId(requesterWorkspaceId);
      return backupClient.createBackup(body, "v0.2");
    } catch (ApiException e) {
      LOGGER.warn("Error from remote WDS: {}", e.getMessage());
      throw new WorkspaceDataServiceException(e);
    }
  }

  /** Checks status of a backup in source workspace data service. */
  public BackupJob checkBackupStatus(String token, UUID trackingId) {
    var backupClient =
        this.workspaceDataServiceClientFactory.getBackupClient(token, workspaceDataServiceUrl);
    try {
      return backupClient.getBackupStatus("v0.2", trackingId);
    } catch (ApiException e) {
      throw new WorkspaceDataServiceException(e);
    }
  }
}
