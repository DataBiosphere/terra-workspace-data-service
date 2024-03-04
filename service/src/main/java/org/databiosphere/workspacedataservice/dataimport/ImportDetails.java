package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;

public record ImportDetails(UUID jobId, String userEmail, UUID collectionId, String prefix) {

  public ImportDetails(UUID collectionId, String prefix) {
    this(null, null, collectionId, prefix);
  }
}
