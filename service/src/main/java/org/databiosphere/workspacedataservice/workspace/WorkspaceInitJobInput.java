package org.databiosphere.workspacedataservice.workspace;

import jakarta.validation.constraints.NotNull;
import java.io.Serializable;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.springframework.lang.Nullable;

/**
 * @param workspaceId the workspace being initialized
 * @param sourceWorkspaceId the workspace being cloned, if any. May be null.
 */
public record WorkspaceInitJobInput(
    @NotNull WorkspaceId workspaceId, @Nullable WorkspaceId sourceWorkspaceId)
    implements JobInput, Serializable {}
