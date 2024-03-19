package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

public class RawlsSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final RawlsClient rawlsClient;
  private final RestClientRetry restClientRetry;
  private final ActivityLogger activityLogger;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsSnapshotSupport.class);

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

  // TODO (AJ-1705): Filter out snapshots that do NOT have purpose:policy
  private SnapshotListResponse enumerateDataRepoSnapshotReferences(int offset, int pageSize) {
    RestClientRetry.RestCall<SnapshotListResponse> restCall =
        (() -> rawlsClient.enumerateDataRepoSnapshotReferences(workspaceId.id(), offset, pageSize));
    return restClientRetry.withRetryAndErrorHandling(
        restCall, "Rawls.enumerateDataRepoSnapshotReferences");
  }

  public List<UUID> existingPolicySnapshotIds(int pageSize) {
    return extractSnapshotIds(listAllSnapshots(pageSize));
  }

  @VisibleForTesting
  List<UUID> extractSnapshotIds(List<DataRepoSnapshotResource> snapshotList) {
    return snapshotList.stream()
        .map(this::safeGetSnapshotId)
        .filter(Objects::nonNull)
        .distinct()
        .toList();
  }

  /**
   * Given a DataRepoSnapshotResource representing a snapshot reference, retrieve that snapshot's
   * UUID.
   *
   * @param snapshotResource the object in which to find a snapshotId
   * @return the snapshotId if found, else null
   */
  @Nullable
  @VisibleForTesting
  protected UUID safeGetSnapshotId(DataRepoSnapshotResource snapshotResource) {
    DataRepoSnapshotAttributes dataRepoSnapshot = snapshotResource.getAttributes();
    if (dataRepoSnapshot != null) {
      String snapshotIdStr = dataRepoSnapshot.getSnapshot();
      try {
        return UUID.fromString(snapshotIdStr);
      } catch (Exception e) {

        String resourceId = "unknown";
        try {
          resourceId = snapshotResource.getMetadata().getResourceId().toString();
        } catch (Exception inner) {
          // something is exceptionally funky about this resource.
          resourceId = inner.getMessage();
        }
        LOGGER.warn(
            "Processed a DataRepoSnapshotResource [%s] that did not contain a valid snapshotId"
                .formatted(resourceId));
      }
    }
    return null;
  }

  /**
   * Query for the full list of referenced snapshots in this workspace, paginating as necessary.
   *
   * @param pageSize how many references to return in each paginated request
   * @return the full list of snapshot references in this workspace
   */
  @VisibleForTesting
  protected List<DataRepoSnapshotResource> listAllSnapshots(int pageSize) {
    int offset = 0;
    final int hardLimit = 10000; // under no circumstances return more than this many snapshots

    List<DataRepoSnapshotResource> finalList = new ArrayList<>(); // collect our results

    while (offset < hardLimit) {
      // get a page of results
      SnapshotListResponse thisPage = enumerateDataRepoSnapshotReferences(offset, pageSize);

      List<DataRepoSnapshotResource> page = thisPage.gcpDataRepoSnapshots();

      // add this page of results to our collector
      finalList.addAll(page);

      if (page.size() < pageSize) {
        // fewer results than we requested; this is the last page of results
        return finalList;
      } else {
        // bump our offset and request another page of results
        offset += pageSize;
      }
    }

    throw new DataImportException(
        "Exceeded hard limit of %d for number of pre-existing snapshot references"
            .formatted(hardLimit));
  }
}
