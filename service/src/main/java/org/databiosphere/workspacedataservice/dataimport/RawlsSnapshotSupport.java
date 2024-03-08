package org.databiosphere.workspacedataservice.dataimport;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class RawlsSnapshotSupport implements SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final RawlsClient rawlsClient;
  private final RestClientRetry restClientRetry;
  private final ActivityLogger activityLogger;

  public RawlsSnapshotSupport(
      WorkspaceId workspaceId,
      RawlsClient rawlsClient,
      RestClientRetry restClientRetry,
      ActivityLogger activityLogger) {
    this.workspaceId = workspaceId;
    this.rawlsClient = rawlsClient;
    this.activityLogger = activityLogger;
    this.restClientRetry = restClientRetry;
  }

  @Override
  public List<UUID> existingPolicySnapshotIds(int pageSize) {
    // TODO: implement
    return null;
  }

  @Override
  public void linkSnapshots(Set<UUID> snapshotIds) {
    // TODO: implement
  }
}
