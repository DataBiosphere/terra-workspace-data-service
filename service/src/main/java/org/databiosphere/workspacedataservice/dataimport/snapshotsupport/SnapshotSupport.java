package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.CloningInstructionsEnum;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import bio.terra.workspace.model.ResourceMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

public abstract class SnapshotSupport {

  private static final String DEFAULT_PRIMARY_KEY = "datarepo_row_id";

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotSupport.class);

  @VisibleForTesting
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
  @VisibleForTesting
  protected UUID safeGetSnapshotId(ResourceDescription resourceDescription) {
    ResourceAttributesUnion resourceAttributes = resourceDescription.getResourceAttributes();
    if (resourceAttributes != null) {
      DataRepoSnapshotAttributes dataRepoSnapshot = resourceAttributes.getGcpDataRepoSnapshot();
      if (dataRepoSnapshot != null) {
        String snapshotIdStr = dataRepoSnapshot.getSnapshot();
        try {
          return UUID.fromString(snapshotIdStr);
        } catch (Exception e) {

          String resourceId;
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

  /**
   * Given a list of snapshot ids, create references from the workspace to the snapshot for each id
   * that does not already have a reference.
   *
   * @param snapshotIds the list of snapshot ids to create or verify references.
   */
  public SnapshotLinkResult linkSnapshots(Set<UUID> snapshotIds) {
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
    int successfulLinks = 0;
    for (UUID uuid : newSnapshotIds) {
      try {
        linkSnapshot(uuid);
        successfulLinks++;
      } catch (RestException re) {
        throw new DataImportException("Error processing data import: " + re.getMessage(), re);
      }
    }

    return new SnapshotLinkResult(snapshotIds.size(), successfulLinks);
  }

  /**
   * Given a single snapshotId, create a reference to that snapshot from the current workspace.
   *
   * @param snapshotId id of the snapshot to reference
   */
  protected abstract void linkSnapshot(UUID snapshotId);

  /**
   * Get the list of unique snapshotIds referenced by the current workspace.
   *
   * @param pageSize how many references to return in each paginated request
   * @return the list of unique ids for all pre-existing snapshot references
   */
  @VisibleForTesting
  public List<UUID> existingPolicySnapshotIds(int pageSize) {
    List<ResourceDescription> allSnapshots = listAllSnapshots(pageSize).getResources();
    Stream<ResourceDescription> policySnapshots =
        allSnapshots.stream()
            .filter(this::isResourceReferenceForPolicy)
            .filter(this::isResourceReferenceCloned);
    return extractSnapshotIds(policySnapshots).toList();
  }

  /** Does the given resource have cloning instructions set to "COPY_REFERENCE". */
  private boolean isResourceReferenceCloned(ResourceDescription resource) {
    return Optional.of(resource)
        .map(ResourceDescription::getMetadata)
        .map(ResourceMetadata::getCloningInstructions)
        .map(CloningInstructionsEnum.REFERENCE::equals)
        .orElse(false);
  }

  /** Does the given resource have a "purpose: policy" property? */
  private boolean isResourceReferenceForPolicy(ResourceDescription resource) {
    return Optional.of(resource)
        .map(ResourceDescription::getMetadata)
        .map(ResourceMetadata::getProperties)
        .map(
            properties ->
                properties.stream()
                    .anyMatch(
                        property ->
                            WorkspaceManagerDao.PROP_PURPOSE.equals(property.getKey())
                                && WorkspaceManagerDao.PURPOSE_POLICY.equals(property.getValue())))
        .orElse(false);
  }

  /**
   * Given a list of TDR tables, find the primary keys for those tables.
   *
   * @param tables the TDR model to inspect
   * @return map of table name->primary key
   */
  public Map<RecordType, String> identifyPrimaryKeys(List<TableModel> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                tableModel -> RecordType.valueOf(tableModel.getName()),
                tableModel -> identifyPrimaryKey(tableModel.getPrimaryKey())));
  }

  /**
   * Given a stream of ResourceDescriptions for snapshot references, find all the valid ids of
   * referenced snapshots in the stream.
   *
   * @param snapshots a stream of snapshot references
   * @return unique snapshot ids in the provided snapshot references
   */
  @VisibleForTesting
  Stream<UUID> extractSnapshotIds(Stream<ResourceDescription> snapshots) {
    return snapshots.map(this::safeGetSnapshotId).filter(Objects::nonNull).distinct();
  }

  /**
   * Given the primary keys specified by a TDR table, return a primary key usable by WDS. WDS
   * requires a single primary key, while TDR allows compound keys and missing keys. This method
   * will return a sensible default if the TDR model is not a single key.
   *
   * @param snapshotKeys the TDR primary keys to inspect
   * @return the primary key to be used by WDS
   */
  @VisibleForTesting
  String identifyPrimaryKey(@Nullable List<String> snapshotKeys) {
    if (snapshotKeys != null && snapshotKeys.size() == 1) {
      return snapshotKeys.get(0);
    }
    return DEFAULT_PRIMARY_KEY;
  }

  /**
   * Query for the full list of referenced snapshots in this workspace, paginating as necessary.
   *
   * @param pageSize how many references to return in each paginated request
   * @return the full list of snapshot references in this workspace
   */
  // TODO (AJ-1705): Filter out snapshots that do NOT have purpose:policy
  @VisibleForTesting
  ResourceList listAllSnapshots(int pageSize) {
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
   * Query a source system (WSM, Rawls) for a single page's worth of snapshot references.
   *
   * @param offset pagination offset; used when the current workspace has many references
   * @param pageSize pagination page size; used when the current workspace has many references
   * @return the page of references from the source system
   */
  protected abstract ResourceList enumerateDataRepoSnapshotReferences(int offset, int pageSize);
}
