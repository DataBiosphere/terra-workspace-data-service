package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;

public record ImportDetails(
    UUID jobId,
    Supplier<String> userEmailSupplier,
    UUID collectionId,
    PrefixStrategy prefixStrategy) {

  private static final Supplier<String> DEFAULT_EMAIL = () -> "unknown";
  private static final UUID DEFAULT_JOB_ID =
      UUID.fromString("00000000-0000-0000-0000-000000000000");

  /**
   * Convenience constructor that sets jobId and userEmail to defaults
   *
   * @param collectionId target collection
   * @param prefixStrategy strategy for potentially renaming attributes
   */
  public ImportDetails(UUID collectionId, PrefixStrategy prefixStrategy) {
    this(DEFAULT_JOB_ID, DEFAULT_EMAIL, collectionId, prefixStrategy);
  }

  /**
   * Convenience constructor that sets jobId and userEmail to defaults, and sets prefixStrategy to
   * NONE
   *
   * @param collectionId target collection
   */
  public ImportDetails(UUID collectionId) {
    this(collectionId, PrefixStrategy.NONE);
  }
}
