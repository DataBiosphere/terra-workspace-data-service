package org.databiosphere.workspacedataservice.rawls;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.List;

public record SnapshotListResponse(List<DataRepoSnapshotResource> gcpDataRepoSnapshots) {}
