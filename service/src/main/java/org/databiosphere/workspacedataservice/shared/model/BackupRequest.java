package org.databiosphere.workspacedataservice.shared.model;

import java.util.UUID;

public record BackupRequest(UUID requestingWorkspaceId, String description) {
}
