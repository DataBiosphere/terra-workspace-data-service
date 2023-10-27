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
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;

public class PfbQuartzJobSupport {

  public static List<UUID> existingPolicySnapshotIds(
      UUID workspaceId, int pageSize, WorkspaceManagerDao wsmDao, RestClientRetry restClientRetry) {
    return extractSnapshotIds(listAllSnapshots(workspaceId, pageSize, wsmDao, restClientRetry));
  }

  public static List<UUID> extractSnapshotIds(ResourceList resourceList) {
    return resourceList.getResources().stream()
        .map(PfbQuartzJobSupport::safeGetSnapshotId)
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
  public static ResourceList listAllSnapshots(
      UUID workspaceId, int pageSize, WorkspaceManagerDao wsmDao, RestClientRetry restClientRetry) {
    final AtomicInteger offset = new AtomicInteger(0);

    ResourceList finalList = new ResourceList(); // collect our results

    while (true) {
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
  }

  /**
   * Given a ResourceDescription representing a snapshot reference, retrieve that snapshot's UUID.
   *
   * @param resourceDescription the WSM object in which to find a snapshotId
   * @return the snapshotId if found, else null
   */
  public static UUID safeGetSnapshotId(ResourceDescription resourceDescription) {
    ResourceAttributesUnion resourceAttributes = resourceDescription.getResourceAttributes();
    if (resourceAttributes != null) {
      DataRepoSnapshotAttributes dataRepoSnapshot = resourceAttributes.getGcpDataRepoSnapshot();
      if (dataRepoSnapshot != null) {
        String snapshotIdStr = dataRepoSnapshot.getSnapshot();
        try {
          return UUID.fromString(snapshotIdStr);
        } catch (Exception e) {
          // TODO: what to do here?
        }
      }
    }
    return null;
  }
}
