package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Component;

@Component
@ControlPlane
public class RawlsSnapshotSupportFactory implements SnapshotSupportFactory {

  private final RestClientRetry restClientRetry;
  private final ActivityLogger activityLogger;
  private final RawlsClient rawlsClient;

  public RawlsSnapshotSupportFactory(
      RestClientRetry restClientRetry, ActivityLogger activityLogger, RawlsClient rawlsClient) {
    this.restClientRetry = restClientRetry;
    this.activityLogger = activityLogger;
    this.rawlsClient = rawlsClient;
  }

  public SnapshotSupport buildSnapshotSupport(WorkspaceId workspaceId) {
    return new RawlsSnapshotSupport(workspaceId, rawlsClient, restClientRetry, activityLogger);
  }
}
