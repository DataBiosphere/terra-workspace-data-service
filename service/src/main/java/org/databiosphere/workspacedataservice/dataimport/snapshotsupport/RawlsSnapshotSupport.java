package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.workspace.model.ResourceList;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawlsSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final RawlsClient rawlsClient;
  private final RestClientRetry restClientRetry;
  private final ActivityLogger activityLogger;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

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

  protected void linkSnapshot(UUID snapshotId) {
    RestClientRetry.VoidRestCall voidRestCall =
        (() -> rawlsClient.createSnapshotReference(workspaceId.id(), snapshotId));
    restClientRetry.withRetryAndErrorHandling(voidRestCall, "Rawls.createSnapshotReference");
    activityLogger.saveEventForCurrentUser(
        user -> user.linked().snapshotReference().withUuid(snapshotId));
  }

  protected ResourceList enumerateDataRepoSnapshotReferences(int offset, int pageSize) {
    RestClientRetry.RestCall<ResourceList> restCall =
        (() -> rawlsClient.enumerateDataRepoSnapshotReferences(workspaceId.id(), offset, pageSize));
    return restClientRetry.withRetryAndErrorHandling(
        restCall, "Rawls.enumerateDataRepoSnapshotReferences");
  }
}
