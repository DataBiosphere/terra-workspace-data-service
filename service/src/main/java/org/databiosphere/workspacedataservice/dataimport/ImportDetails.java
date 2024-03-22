package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;

public record ImportDetails(
    UUID jobId,
    Supplier<String> userEmailSupplier,
    UUID collectionId,
    PrefixStrategy prefixStrategy) {

  private static final Supplier<String> DEFAULT_EMAIL_SUPPLIER = () -> "unknown";
  private static final UUID DEFAULT_JOB_ID =
      UUID.fromString("00000000-0000-0000-0000-000000000000");

  /**
   * Convenience constructor that sets jobId and userEmail to defaults, and sets prefixStrategy to
   * NONE. Used by TSV and JSON APIs, which are not async and thus do not have job ids
   *
   * @param collectionId target collection
   */
  public ImportDetails(UUID collectionId) {
    this(DEFAULT_JOB_ID, DEFAULT_EMAIL_SUPPLIER, collectionId, PrefixStrategy.NONE);
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
