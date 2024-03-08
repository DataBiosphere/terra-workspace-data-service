package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.TableModel;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
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

  @Override
  public Map<RecordType, String> identifyPrimaryKeys(List<TableModel> tables) {
    // TODO implement or determine whether this belongs here
    return null;
  }

  @Override
  public Multimap<RecordType, RelationshipModel> identifyRelations(
      List<RelationshipModel> relationshipModels) {
    // TODO implement
    return null;
  }
}
