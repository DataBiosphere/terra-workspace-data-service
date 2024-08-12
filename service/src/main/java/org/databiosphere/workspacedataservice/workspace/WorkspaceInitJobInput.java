package org.databiosphere.workspacedataservice.workspace;

import jakarta.validation.constraints.NotNull;
import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.springframework.lang.Nullable;

public class WorkspaceInitJobInput implements JobInput, Serializable {
  @Serial private static final long serialVersionUID = 0L;

  // the workspace being initialized
  @NotNull private final WorkspaceId workspaceId;

  @Nullable
  public UUID getSourceWorkspaceId() {
    return sourceWorkspaceId;
  }

  public @NotNull WorkspaceId getWorkspaceId() {
    return workspaceId;
  }

  // the workspace being cloned, if any. May be null.
  @Nullable private final UUID sourceWorkspaceId;

  public WorkspaceInitJobInput(WorkspaceId workspaceId) {
    this.workspaceId = workspaceId;
    this.sourceWorkspaceId = null;
  }

  public WorkspaceInitJobInput(WorkspaceId workspaceId, @Nullable UUID sourceWorkspaceId) {
    this.workspaceId = workspaceId;
    this.sourceWorkspaceId = sourceWorkspaceId;
  }
}
