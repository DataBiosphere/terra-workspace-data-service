package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public record ImportDetails(
    UUID jobId,
    Supplier<String> userEmailSupplier,
    WorkspaceId workspaceId,
    CollectionId collectionId,
    PrefixStrategy prefixStrategy,
    ImportJobInput importJobInput) {

  /**
   * Override to ensure we never serialize the userEmailSupplier, which can contain an auth token.
   *
   * @return serialized String
   */
  @Override
  public String toString() {
    return "ImportDetails{"
        + "jobId="
        + jobId
        + ", userEmailSupplier=Supplier<String>"
        + ", workspaceId="
        + workspaceId
        + ", collectionId="
        + collectionId
        + ", prefixStrategy="
        + prefixStrategy
        + ", importJobInput="
        + importJobInput
        + '}';
  }
}
