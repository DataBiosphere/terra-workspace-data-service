package org.databiosphere.workspacedataservice.dataimport;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface SnapshotSupport {

  List<UUID> existingPolicySnapshotIds(int pageSize);

  void linkSnapshots(Set<UUID> snapshotIds);
}
