package org.databiosphere.workspacedataservice.dataimport;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TdrSnapshotSupport {

  private final UUID workspaceId;
  private final WorkspaceManagerDao wsmDao;
  private final RestClientRetry restClientRetry;

  private static final String DEFAULT_PRIMARY_KEY = "datarepo_row_id";

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public TdrSnapshotSupport(
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
  @VisibleForTesting
  public List<UUID> existingPolicySnapshotIds(int pageSize) {
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

    throw new DataImportException(
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
          String resourceId = "unknown";
          try {
            resourceId = resourceDescription.getMetadata().getResourceId().toString();
          } catch (Exception inner) {
            // something is exceptionally funky about this resource.
            resourceId = inner.getMessage();
          }
          logger.warn(
              "Processed a ResourceDescription [%s] for workspace %s that did not contain a valid snapshotId"
                  .formatted(resourceId, workspaceId));
        }
      }
    }
    return null;
  }

  /**
   * Given a list of snapshot ids, create references from the workspace to the snapshot for each id
   * that does not already have a reference.
   *
   * @param snapshotIds the list of snapshot ids to create or verify references.
   */
  protected void linkSnapshots(Set<UUID> snapshotIds) {
    // list existing snapshots linked to this workspace
    List<UUID> existingSnapshotIds = existingPolicySnapshotIds(/* pageSize= */ 50);
    // find the snapshots that are not already linked to this workspace
    List<UUID> newSnapshotIds =
        snapshotIds.stream().filter(id -> !existingSnapshotIds.contains(id)).toList();

    logger.info(
        "Import data contains {} snapshot ids. {} of these are already linked to the workspace; {} new links will be created.",
        snapshotIds.size(),
        snapshotIds.size() - newSnapshotIds.size(),
        newSnapshotIds.size());

    // pass snapshotIds to WSM
    for (UUID uuid : newSnapshotIds) {
      try {
        RestClientRetry.VoidRestCall voidRestCall =
            (() -> wsmDao.linkSnapshotForPolicy(new SnapshotModel().id(uuid)));
        restClientRetry.withRetryAndErrorHandling(
            voidRestCall, "WSM.createDataRepoSnapshotReference");
      } catch (RestException re) {
        throw new DataImportException("Error processing data import: " + re.getMessage(), re);
      }
    }
  }

  // TODO AJ-1013 unit tests
  Map<RecordType, String> identifyPrimaryKeys(List<TableModel> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                tableModel -> RecordType.valueOf(tableModel.getName()),
                tableModel -> identifyPrimaryKey(tableModel.getPrimaryKey())));
  }

  // TODO AJ-1013 unit tests
  String identifyPrimaryKey(List<String> snapshotKeys) {
    if (snapshotKeys.size() == 1) {
      return snapshotKeys.get(0);
    }
    return DEFAULT_PRIMARY_KEY;
  }

  String getDefaultPrimaryKey() {
    return DEFAULT_PRIMARY_KEY;
  }

  // TODO AJ-1013 unit tests

  /**
   * Returns a Multimap of RecordType -> RelationshipModel, indicating all the outbound relations
   * for any given RecordType in this snapshot model.
   *
   * @param relationshipModels relationship models from the TDR manifest
   * @return the relationship models, mapped by RecordType
   */
  Multimap<RecordType, RelationshipModel> identifyRelations(
      List<RelationshipModel> relationshipModels) {
    return Multimaps.index(
        relationshipModels,
        relationshipModel -> RecordType.valueOf(relationshipModel.getFrom().getTable()));
  }
}
