package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.ResourceList;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;

public class WsmSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final WorkspaceManagerDao wsmDao;
  private final RestClientRetry restClientRetry;
  private final ActivityLogger activityLogger;

  public WsmSnapshotSupport(
      WorkspaceId workspaceId,
      WorkspaceManagerDao wsmDao,
      RestClientRetry restClientRetry,
      ActivityLogger activityLogger) {
    this.workspaceId = workspaceId;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
    this.activityLogger = activityLogger;
  }

  @Override
  protected ResourceList enumerateDataRepoSnapshotReferences(int offset, int pageSize) {
    RestClientRetry.RestCall<ResourceList> restCall =
        (() -> wsmDao.enumerateDataRepoSnapshotReferences(workspaceId.id(), offset, pageSize));
    return restClientRetry.withRetryAndErrorHandling(
        restCall, "WSM.enumerateDataRepoSnapshotReferences");
  }

  @Override
  protected void linkSnapshot(UUID snapshotId) {
    RestClientRetry.VoidRestCall voidRestCall =
        (() -> wsmDao.linkSnapshotForPolicy(workspaceId, new SnapshotModel().id(snapshotId)));
    restClientRetry.withRetryAndErrorHandling(voidRestCall, "WSM.createDataRepoSnapshotReference");
    activityLogger.saveEventForCurrentUser(
        user -> user.linked().snapshotReference().withUuid(snapshotId));
  }
}
