package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public record RelationAttribute(RecordType targetType, String targetId) {

  @Override
  public String toString() {
    return RelationUtils.createRelationString(targetType, targetId);
  }
}
