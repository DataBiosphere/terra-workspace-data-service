package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface SnapshotSupportFactory {

  SnapshotSupport buildSnapshotSupport(WorkspaceId workspaceId);
}
