package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;

public record ImportDetails(UUID jobId, String userEmail, UUID workspaceId, String prefix) {

  public ImportDetails(UUID workspaceId, String prefix) {
    this(null, null, workspaceId, prefix);
  }
}
