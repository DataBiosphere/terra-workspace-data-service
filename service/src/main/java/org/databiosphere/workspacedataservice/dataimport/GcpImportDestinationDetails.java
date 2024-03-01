package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;

public record GcpImportDestinationDetails(UUID jobId, String userEmail, UUID workspaceId) {
  public static GcpImportDestinationDetails empty() {
    return new GcpImportDestinationDetails(null, null, null);
  }
}
