package org.databiosphere.workspacedataservice.rawls;

import java.util.UUID;

/**
 * Represents a snapshot reference to be created in a GCP workspace via the Rawls API
 *
 * @param name name for this reference
 * @param description description for this reference
 * @param snapshotId the UUID of the snapshot to reference
 */
public record NamedDataRepoSnapshot(String name, String description, UUID snapshotId) {

  public static NamedDataRepoSnapshot forSnapshotId(UUID snapshotId) {
    String referenceName = "%s-policy".formatted(snapshotId);
    String referenceDescription = "created at %s".formatted(System.currentTimeMillis());
    return new NamedDataRepoSnapshot(referenceName, referenceDescription, snapshotId);
  }
}
