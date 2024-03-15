package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

public abstract class SnapshotSupport {

  protected static final String DEFAULT_PRIMARY_KEY = "datarepo_row_id";

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotSupport.class);

  String getDefaultPrimaryKey() {
    return DEFAULT_PRIMARY_KEY;
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
          // TODO The only thing this method requires from the implementing class is workspaceId
          // Should we therefore create a method that returns workspaceId or otherwise call the
          // implementing clas
          // Or just implement it multiple times?
          String resourceId = "unknown";
          try {
            resourceId = resourceDescription.getMetadata().getResourceId().toString();
          } catch (Exception inner) {
            // something is exceptionally funky about this resource.
            resourceId = inner.getMessage();
          }
          //          logger.warn(
          //              "Processed a ResourceDescription [%s] for workspace %s that did not
          // contain a valid snapshotId"
          //                  .formatted(resourceId, workspaceId));
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
  public void linkSnapshots(Set<UUID> snapshotIds) {
    // list existing snapshots linked to this workspace
    Set<UUID> existingSnapshotIds = Set.copyOf(existingPolicySnapshotIds(/* pageSize= */ 50));
    // find the snapshots that are not already linked to this workspace
    Set<UUID> newSnapshotIds = Sets.difference(snapshotIds, existingSnapshotIds);

    LOGGER.info(
        "Import data contains {} snapshot ids. {} of these are already linked to the workspace; {} new links will be created.",
        snapshotIds.size(),
        snapshotIds.size() - newSnapshotIds.size(),
        newSnapshotIds.size());

    // pass snapshotIds to underlying client to link
    for (UUID uuid : newSnapshotIds) {
      try {
        linkSnapshot(uuid);
      } catch (RestException re) {
        throw new DataImportException("Error processing data import: " + re.getMessage(), re);
      }
    }
  }

  protected abstract void linkSnapshot(UUID snapshotId);

  /**
   * Query for the full list of referenced snapshots in this workspace, then return the list of
   * unique snapshotIds from those references. Calls Rawls or WSM depending on the implementation of
   * listAllSnapshots
   *
   * @param pageSize how many references to return in each paginated request
   * @return the list of unique ids for all pre-existing snapshot references
   */
  public List<UUID> existingPolicySnapshotIds(int pageSize) {
    return extractSnapshotIds(listAllSnapshots(pageSize));
  }

  public Map<RecordType, String> identifyPrimaryKeys(List<TableModel> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                tableModel -> RecordType.valueOf(tableModel.getName()),
                tableModel -> identifyPrimaryKey(tableModel.getPrimaryKey())));
  }

  public Multimap<RecordType, RelationshipModel> identifyRelations(
      List<RelationshipModel> relationshipModels) {
    return Multimaps.index(
        relationshipModels,
        relationshipModel -> RecordType.valueOf(relationshipModel.getFrom().getTable()));
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

  String identifyPrimaryKey(@Nullable List<String> snapshotKeys) {
    if (snapshotKeys != null && snapshotKeys.size() == 1) {
      return snapshotKeys.get(0);
    }
    return DEFAULT_PRIMARY_KEY;
  }

  /**
   * Query for the full list of referenced snapshots in this workspace, paginating as necessary.
   * Calls Rawls or WSM depending on implementation of enumerateDataRepoSnapshotReferences
   *
   * @param pageSize how many references to return in each paginated request
   * @return the full list of snapshot references in this workspace
   */
  protected ResourceList listAllSnapshots(int pageSize) {
    final AtomicInteger offset = new AtomicInteger(0);
    final int hardLimit = 10000; // under no circumstances return more than this many snapshots

    ResourceList finalList = new ResourceList(); // collect our results

    while (offset.get() < hardLimit) {
      // get a page of results
      ResourceList thisPage = enumerateDataRepoSnapshotReferences(offset.get(), pageSize);

      // add this page of results to our collector
      finalList.getResources().addAll(thisPage.getResources());

      if (thisPage.getResources().size() < pageSize) {
        // fewer results than we requested; this is the last page of results
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

  protected abstract ResourceList enumerateDataRepoSnapshotReferences(int offset, int limit);
}
