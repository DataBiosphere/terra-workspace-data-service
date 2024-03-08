package org.databiosphere.workspacedataservice.dataimport;

import jakarta.annotation.Nullable;
import java.util.UUID;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;

public record ImportDetails(
    @Nullable UUID jobId,
    @Nullable String userEmail,
    UUID collectionId,
    PrefixStrategy prefixStrategy) {

  public ImportDetails(UUID collectionId, PrefixStrategy prefixStrategy) {
    this(null, null, collectionId, prefixStrategy);
  }
}
