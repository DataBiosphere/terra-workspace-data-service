package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.TableModel;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public interface SnapshotSupport {

  List<UUID> existingPolicySnapshotIds(int pageSize);

  void linkSnapshots(Set<UUID> snapshotIds);

  // TODO as noted in WsmSnapshotSupport, this might not belong here
  Map<RecordType, String> identifyPrimaryKeys(List<TableModel> tables);

  Multimap<RecordType, RelationshipModel> identifyRelations(
      List<RelationshipModel> relationshipModels);
}
