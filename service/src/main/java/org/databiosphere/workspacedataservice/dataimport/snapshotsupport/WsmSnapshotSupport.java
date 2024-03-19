package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

public class WsmSnapshotSupport extends SnapshotSupport {

  private final WorkspaceId workspaceId;
  private final WorkspaceManagerDao wsmDao;
  private final RestClientRetry restClientRetry;
  private final ActivityLogger activityLogger;

  private static final Logger LOGGER = LoggerFactory.getLogger(WsmSnapshotSupport.class);

  public WsmSnapshotSupport(
      WorkspaceId workspaceId,
      WorkspaceManagerDao wsmDao,
      RestClientRetry restClientRetry,
      ActivityLogger activityLogger) {
    this.workspaceId = workspaceId;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
    this.activityLogger = activityLogger;
  }

  protected ResourceList enumerateDataRepoSnapshotReferences(int offset, int pageSize) {
    RestClientRetry.RestCall<ResourceList> restCall =
        (() -> wsmDao.enumerateDataRepoSnapshotReferences(workspaceId.id(), offset, pageSize));
    return restClientRetry.withRetryAndErrorHandling(
        restCall, "WSM.enumerateDataRepoSnapshotReferences");
  }

  protected void linkSnapshot(UUID snapshotId) {
    RestClientRetry.VoidRestCall voidRestCall =
        (() -> wsmDao.linkSnapshotForPolicy(workspaceId, new SnapshotModel().id(snapshotId)));
    restClientRetry.withRetryAndErrorHandling(voidRestCall, "WSM.createDataRepoSnapshotReference");
    activityLogger.saveEventForCurrentUser(
        user -> user.linked().snapshotReference().withUuid(snapshotId));
  }

  public List<UUID> existingPolicySnapshotIds(int pageSize) {
    return extractSnapshotIds(listAllSnapshots(pageSize));
  }

  /**
   * Query for the full list of referenced snapshots in this workspace, paginating as necessary.
   *
   * @param pageSize how many references to return in each paginated request
   * @return the full list of snapshot references in this workspace
   */
  protected ResourceList listAllSnapshots(int pageSize) {
    int offset = 0;
    final int hardLimit = 10000; // under no circumstances return more than this many snapshots

    ResourceList finalList = new ResourceList(); // collect our results

    while (offset < hardLimit) {
      // get a page of results
      ResourceList thisPage = enumerateDataRepoSnapshotReferences(offset, pageSize);

      // add this page of results to our collector
      finalList.getResources().addAll(thisPage.getResources());

      if (thisPage.getResources().size() < pageSize) {
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

  /**
   * Given a ResourceList, find all the valid ids of referenced snapshots in that list
   *
   * @param resourceList the list in which to look for snapshotIds
   * @return the list of unique ids in the provided list
   */
  @VisibleForTesting
  List<UUID> extractSnapshotIds(ResourceList resourceList) {
    return resourceList.getResources().stream()
        .map(this::safeGetSnapshotId)
        .filter(Objects::nonNull)
        .distinct()
        .toList();
  }

  /**
   * Given a ResourceDescription representing a snapshot reference, retrieve that snapshot's UUID.
   *
   * @param resourceDescription the object in which to find a snapshotId
   * @return the snapshotId if found, else null
   */
  @Nullable
  protected UUID safeGetSnapshotId(ResourceDescription resourceDescription) {
    ResourceAttributesUnion resourceAttributes = resourceDescription.getResourceAttributes();
    if (resourceAttributes != null) {
      DataRepoSnapshotAttributes dataRepoSnapshot = resourceAttributes.getGcpDataRepoSnapshot();
      if (dataRepoSnapshot != null) {
        String snapshotIdStr = dataRepoSnapshot.getSnapshot();
        try {
          return UUID.fromString(snapshotIdStr);
        } catch (Exception e) {

          String resourceId = "unknown";
          try {
            resourceId = resourceDescription.getMetadata().getResourceId().toString();
          } catch (Exception inner) {
            // something is exceptionally funky about this resource.
            resourceId = inner.getMessage();
          }
          LOGGER.warn(
              "Processed a ResourceDescription [%s] that did not contain a valid snapshotId"
                  .formatted(resourceId));
        }
      }
    }
    return null;
  }
}
