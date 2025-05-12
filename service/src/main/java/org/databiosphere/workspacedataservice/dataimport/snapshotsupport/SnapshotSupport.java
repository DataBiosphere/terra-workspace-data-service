package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

public abstract class SnapshotSupport {

  /**
   * indicates the purpose of a snapshot reference - e.g. is it created for the sole purpose of
   * linking policies.
   */
  public static final String PROP_PURPOSE = "purpose";

  public static final String PURPOSE_POLICY = "policy";
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

    // only call Rawls if we found snapshots
    if (!snapshotIds.isEmpty()) {
      try {
        linkSnapshots(snapshotIds.stream().toList());
      } catch (RestException re) {
        throw new DataImportException("Error processing data import: " + re.getMessage(), re);
      }
    }

    // TODO do we need to figure out how many succeeded vs failed?
    return new SnapshotLinkResult(snapshotIds.size(), snapshotIds.size());
  }

  /**
   * Given a list of snapshotIds, create a reference to each of them from the current workspace.
   *
   * @param snapshotIds ids of the snapshots to reference
   */
  protected abstract void linkSnapshots(List<UUID> snapshotIds);

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
}
