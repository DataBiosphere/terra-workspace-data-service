package org.databiosphere.workspacedataservice.workspace;

import jakarta.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Optional;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.springframework.lang.Nullable;

/**
 * JobInput implementation for workspace initialization.
 *
 * @param workspaceId the workspace being initialized
 * @param sourceWorkspaceId the workspace being cloned, if any. May be null.
 */
public record WorkspaceInitJobInput(
    @NotNull WorkspaceId workspaceId, @Nullable WorkspaceId sourceWorkspaceId)
    implements JobInput, Serializable {

  public static WorkspaceInitJobInput from(
      WorkspaceId workspaceId, WorkspaceInitServerModel workspaceInitServerModel) {

    Optional<WorkspaceId> sourceWorkspaceId =
        Optional.ofNullable(workspaceInitServerModel.getClone())
            .flatMap(clone -> Optional.ofNullable(clone.getSourceWorkspaceId()))
            .map(WorkspaceId::of);

    return new WorkspaceInitJobInput(workspaceId, sourceWorkspaceId.orElse(null));
  }
}
