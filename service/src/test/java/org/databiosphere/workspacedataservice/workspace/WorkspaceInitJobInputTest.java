package org.databiosphere.workspacedataservice.workspace;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitCloneServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;

class WorkspaceInitJobInputTest {

  @Test
  void fromNonClone() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    WorkspaceInitCloneServerModel clone = new WorkspaceInitCloneServerModel(null);
    WorkspaceInitServerModel workspaceInitServerModel = new WorkspaceInitServerModel();
    workspaceInitServerModel.setClone(clone);

    WorkspaceInitJobInput expected = new WorkspaceInitJobInput(workspaceId, null);

    WorkspaceInitJobInput actual =
        WorkspaceInitJobInput.from(workspaceId, workspaceInitServerModel);
    assertEquals(expected, actual);
  }

  @Test
  void fromClone() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    WorkspaceId sourceWorkspaceId = WorkspaceId.of(UUID.randomUUID());

    WorkspaceInitCloneServerModel clone = new WorkspaceInitCloneServerModel(sourceWorkspaceId.id());
    WorkspaceInitServerModel workspaceInitServerModel = new WorkspaceInitServerModel();
    workspaceInitServerModel.setClone(clone);

    WorkspaceInitJobInput expected = new WorkspaceInitJobInput(workspaceId, sourceWorkspaceId);

    WorkspaceInitJobInput actual =
        WorkspaceInitJobInput.from(workspaceId, workspaceInitServerModel);
    assertEquals(expected, actual);
  }
}
