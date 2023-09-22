package org.databiosphere.workspacedataservice.shared.model;

import java.util.UUID;

public record BackupRestoreRequest(UUID requestingWorkspaceId, String description) {}
