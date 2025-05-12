package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class RawlsSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final RawlsClient rawlsClient;
  private final ActivityLogger activityLogger;

  public RawlsSnapshotSupport(
      WorkspaceId workspaceId, RawlsClient rawlsClient, ActivityLogger activityLogger) {
    this.workspaceId = workspaceId;
    this.rawlsClient = rawlsClient;
    this.activityLogger = activityLogger;
  }

  @Override
  protected void linkSnapshots(List<UUID> snapshotIds) {
    rawlsClient.createSnapshotReferences(workspaceId.id(), snapshotIds);
    activityLogger.saveEventForCurrentUser(
        user ->
            user.linked()
                .snapshotReference()
                .withIds(snapshotIds.stream().map(UUID::toString).toArray(String[]::new)));
  }
}
