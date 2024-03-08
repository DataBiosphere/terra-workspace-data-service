package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface SnapshotSupportFactory {

  SnapshotSupport buildSnapshotSupport(WorkspaceId workspaceId);
}
