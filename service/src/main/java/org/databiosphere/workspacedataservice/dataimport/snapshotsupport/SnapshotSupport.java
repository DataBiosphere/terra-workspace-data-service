package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.TableModel;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
   * unique snapshotIds from those references. Relies on implementing class
   *
   * @param pageSize how many references to return in each paginated request
   * @return the list of unique ids for all pre-existing snapshot references
   */
  public abstract List<UUID> existingPolicySnapshotIds(int pageSize);

  public Map<RecordType, String> identifyPrimaryKeys(List<TableModel> tables) {
    return tables.stream()
        .collect(
            Collectors.toMap(
                tableModel -> RecordType.valueOf(tableModel.getName()),
                tableModel -> identifyPrimaryKey(tableModel.getPrimaryKey())));
  }

  String identifyPrimaryKey(@Nullable List<String> snapshotKeys) {
    if (snapshotKeys != null && snapshotKeys.size() == 1) {
      return snapshotKeys.get(0);
    }
    return DEFAULT_PRIMARY_KEY;
  }
}
