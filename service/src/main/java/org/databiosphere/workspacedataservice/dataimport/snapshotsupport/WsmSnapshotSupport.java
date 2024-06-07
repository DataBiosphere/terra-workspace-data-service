package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.ResourceList;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;

public class WsmSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final WorkspaceManagerDao wsmDao;
  private final ActivityLogger activityLogger;

  public WsmSnapshotSupport(
      WorkspaceId workspaceId, WorkspaceManagerDao wsmDao, ActivityLogger activityLogger) {
    this.workspaceId = workspaceId;
    this.wsmDao = wsmDao;
    this.activityLogger = activityLogger;
  }

  @Override
  protected ResourceList enumerateDataRepoSnapshotReferences(int offset, int pageSize) {
    return wsmDao.enumerateDataRepoSnapshotReferences(workspaceId, offset, pageSize);
  }

  @Override
  protected void linkSnapshot(UUID snapshotId) {
    wsmDao.linkSnapshotForPolicy(workspaceId, new SnapshotModel().id(snapshotId));
    activityLogger.saveEventForCurrentUser(
        user -> user.linked().snapshotReference().withUuid(snapshotId));
  }
}
