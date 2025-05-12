package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Component;

/** Factory to generate the appropriate {@link SnapshotSupport} for any given workspace. */
@Component
public class MultiCloudSnapshotSupportFactory {

  private final ActivityLogger activityLogger;
  private final RawlsClient rawlsClient;

  public MultiCloudSnapshotSupportFactory(ActivityLogger activityLogger, RawlsClient rawlsClient) {
    this.activityLogger = activityLogger;
    this.rawlsClient = rawlsClient;
  }

  /**
   * Get the {@link SnapshotSupport} for a given workspace. As of 2025, this will always be
   * RawlsSnapshotSupport.
   *
   * @param workspaceId the workspace in question
   * @return the appropriate {@link SnapshotSupport} for the workspace
   */
  public SnapshotSupport buildSnapshotSupport(WorkspaceId workspaceId) {
    return new SnapshotSupport(workspaceId, rawlsClient, activityLogger);
  }
}
