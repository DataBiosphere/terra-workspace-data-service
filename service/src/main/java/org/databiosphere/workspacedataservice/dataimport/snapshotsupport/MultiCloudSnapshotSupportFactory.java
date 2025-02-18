package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.service.WorkspaceService;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.stereotype.Component;

/** Factory to generate the appropriate {@link SnapshotSupport} for any given workspace. */
@Component
public class MultiCloudSnapshotSupportFactory implements SnapshotSupportFactory {

  private final ActivityLogger activityLogger;
  private final RawlsClient rawlsClient;
  private final WorkspaceService workspaceService;
  private final WorkspaceManagerDao wsmDao;

  public MultiCloudSnapshotSupportFactory(
      ActivityLogger activityLogger,
      RawlsClient rawlsClient,
      WorkspaceService workspaceService,
      WorkspaceManagerDao wsmDao) {
    this.activityLogger = activityLogger;
    this.rawlsClient = rawlsClient;
    this.workspaceService = workspaceService;
    this.wsmDao = wsmDao;
  }

  /**
   * Get the {@link SnapshotSupport} for a given workspace. As of 2025, this will always be
   * RawlsSnapshotSupport.
   *
   * @param workspaceId the workspace in question
   * @return the appropriate {@link SnapshotSupport} for the workspace
   */
  @Override
  public SnapshotSupport buildSnapshotSupport(WorkspaceId workspaceId) {
    return new RawlsSnapshotSupport(workspaceId, rawlsClient, activityLogger);
  }
}
