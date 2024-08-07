package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.stereotype.Component;

@Component
@DataPlane
public class WsmSnapshotSupportFactory implements SnapshotSupportFactory {

  private final ActivityLogger activityLogger;
  private final WorkspaceManagerDao wsmDao;

  public WsmSnapshotSupportFactory(ActivityLogger activityLogger, WorkspaceManagerDao wsmDao) {
    this.activityLogger = activityLogger;
    this.wsmDao = wsmDao;
  }

  public SnapshotSupport buildSnapshotSupport(WorkspaceId workspaceId) {
    return new WsmSnapshotSupport(workspaceId, wsmDao, activityLogger);
  }
}
