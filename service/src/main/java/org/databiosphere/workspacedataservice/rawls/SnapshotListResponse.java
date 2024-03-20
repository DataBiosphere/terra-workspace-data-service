package org.databiosphere.workspacedataservice.rawls;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.List;

/**
 * Rawls model representing the list of snapshots referenced by a workspace.
 *
 * @param gcpDataRepoSnapshots the snapshot references
 */
public record SnapshotListResponse(List<DataRepoSnapshotResource> gcpDataRepoSnapshots) {}
