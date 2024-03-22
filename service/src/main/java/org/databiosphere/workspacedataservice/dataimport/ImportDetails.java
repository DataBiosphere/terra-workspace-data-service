package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.springframework.lang.Nullable;

public record ImportDetails(
    @Nullable UUID jobId,
    @Nullable Supplier<String> userEmailSupplier,
    UUID collectionId,
    PrefixStrategy prefixStrategy) {

  /**
   * Convenience constructor that sets jobId and userEmail to defaults, and sets prefixStrategy to
   * NONE. Used by TSV and JSON APIs, which are not async and thus do not have job ids
   *
   * @param collectionId target collection
   */
  public ImportDetails(UUID collectionId) {
    this(null, null, collectionId, PrefixStrategy.NONE);
  }

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
        + ", collectionId="
        + collectionId
        + ", prefixStrategy="
        + prefixStrategy
        + '}';
  }
}
