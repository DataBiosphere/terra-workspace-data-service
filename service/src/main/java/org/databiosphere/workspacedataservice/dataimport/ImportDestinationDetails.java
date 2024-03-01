package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;

public record ImportDestinationDetails(UUID jobId, String userEmail, UUID workspaceId) {
  public static ImportDestinationDetails empty() {
    return new ImportDestinationDetails(null, null, null);
  }
}
