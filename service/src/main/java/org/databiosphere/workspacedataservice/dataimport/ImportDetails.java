package org.databiosphere.workspacedataservice.dataimport;

import jakarta.annotation.Nullable;
import java.util.UUID;

public record ImportDetails(
    @Nullable UUID jobId, @Nullable String userEmail, UUID collectionId, String prefix) {

  public ImportDetails(UUID collectionId, String prefix) {
    this(null, null, collectionId, prefix);
  }
}
