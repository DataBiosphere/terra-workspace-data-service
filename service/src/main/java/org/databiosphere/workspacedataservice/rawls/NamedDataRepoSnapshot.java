package org.databiosphere.workspacedataservice.rawls;

import bio.terra.workspace.model.CloningInstructionsEnum;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupport;

/**
 * Represents a snapshot reference to be created in a GCP workspace via the Rawls API
 *
 * @param name name for this reference
 * @param description description for this reference
 * @param snapshotId the UUID of the snapshot to reference
 * @param cloningInstructions cloning semantics for this snapshot
 * @param properties key-value pairs to associate with the snapshot
 */
public record NamedDataRepoSnapshot(
    String name,
    String description,
    UUID snapshotId,
    CloningInstructionsEnum cloningInstructions,
    Map<String, String> properties) {

  public static NamedDataRepoSnapshot forSnapshotId(UUID snapshotId) {
    String referenceName = "%s-policy".formatted(snapshotId);
    String referenceDescription = "created at %s".formatted(System.currentTimeMillis());
    return new NamedDataRepoSnapshot(
        referenceName,
        referenceDescription,
        snapshotId,
        CloningInstructionsEnum.REFERENCE,
        Map.of(SnapshotSupport.PROP_PURPOSE, SnapshotSupport.PURPOSE_POLICY));
  }
}
