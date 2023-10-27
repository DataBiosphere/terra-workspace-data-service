package org.databiosphere.workspacedataservice.dataimport;

import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.model.exception.PfbImportException;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;

public class PfbQuartzJobSupport {

  private final UUID workspaceId;
  private final WorkspaceManagerDao wsmDao;
  private final RestClientRetry restClientRetry;

  public PfbQuartzJobSupport(
      UUID workspaceId, WorkspaceManagerDao wsmDao, RestClientRetry restClientRetry) {
    this.workspaceId = workspaceId;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
  }

  /**
   * Query WSM for the full list of referenced snapshots in this workspace, then return the list of
   * unique snapshotIds from those references.
   *
   * @param pageSize how many references to return in each paginated request to WSM
   * @return the list of unique ids for all pre-existing snapshot references
   */
  protected List<UUID> existingPolicySnapshotIds(int pageSize) {
    return extractSnapshotIds(listAllSnapshots(pageSize));
  }

  /**
   * Given a ResourceList, find all the valid ids of referenced snapshots in that list
   *
   * @param resourceList the list in which to look for snapshotIds
   * @return the list of unique ids in the provided list
   */
  protected List<UUID> extractSnapshotIds(ResourceList resourceList) {
    return resourceList.getResources().stream()
        .map(this::safeGetSnapshotId)
        .filter(Objects::nonNull)
        .distinct()
        .toList();
  }

  /**
   * Get the full list of all snapshot references for the current workspace. WSM returns these
   * results paginated; this method retrieves pages from WSM and aggregates the results.
   *
   * @param pageSize number of results to return from WSM at once
   * @return the full list of all snapshot references for the workspace.
   */
  protected ResourceList listAllSnapshots(int pageSize) {
    final AtomicInteger offset = new AtomicInteger(0);
    final int hardLimit =
        10000; // under no circumstances return more than this many snapshots from WSM

    ResourceList finalList = new ResourceList(); // collect our results

    while (offset.get() < hardLimit) {
      // get a page of results from WSM
      RestClientRetry.RestCall<ResourceList> restCall =
          (() -> wsmDao.enumerateDataRepoSnapshotReferences(workspaceId, offset.get(), pageSize));
      ResourceList thisPage =
          restClientRetry.withRetryAndErrorHandling(
              restCall, "WSM.enumerateDataRepoSnapshotReferences");

      // add this page of results to our collector
      finalList.getResources().addAll(thisPage.getResources());

      if (thisPage.getResources().size() < pageSize) {
        // fewer results from WSM than we requested; this is the last page of results
        return finalList;
      } else {
        // bump our offset and request another page of results
        offset.addAndGet(pageSize);
      }
    }

    throw new PfbImportException(
        "Exceeded hard limit of %d for number of pre-existing snapshot references"
            .formatted(hardLimit));
  }

  /**
   * Given a ResourceDescription representing a snapshot reference, retrieve that snapshot's UUID.
   *
   * @param resourceDescription the WSM object in which to find a snapshotId
   * @return the snapshotId if found, else null
   */
  protected UUID safeGetSnapshotId(ResourceDescription resourceDescription) {
    ResourceAttributesUnion resourceAttributes = resourceDescription.getResourceAttributes();
    if (resourceAttributes != null) {
      DataRepoSnapshotAttributes dataRepoSnapshot = resourceAttributes.getGcpDataRepoSnapshot();
      if (dataRepoSnapshot != null) {
        String snapshotIdStr = dataRepoSnapshot.getSnapshot();
        try {
          return UUID.fromString(snapshotIdStr);
        } catch (Exception e) {
          // noop; this will return null
        }
      }
    }
    return null;
  }
}
