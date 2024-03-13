package org.databiosphere.workspacedataservice.dataimport;

import jakarta.annotation.Nullable;
import java.util.UUID;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;

public record ImportDetails(
    @Nullable UUID jobId,
    @Nullable String userEmail,
    UUID collectionId,
    PrefixStrategy prefixStrategy) {

  /**
   * Convenience constructor that sets jobId and userEmail to null
   *
   * @param collectionId target collection
   * @param prefixStrategy strategy for potentially renaming attributes
   */
  public ImportDetails(UUID collectionId, PrefixStrategy prefixStrategy) {
    this(null, null, collectionId, prefixStrategy);
  }

  /**
   * Convenience constructor that sets jobId and userEmail to null, and sets prefixStrategy to NONE
   *
   * @param collectionId target collection
   */
  public ImportDetails(UUID collectionId) {
    this(null, null, collectionId, PrefixStrategy.NONE);
  }
}
