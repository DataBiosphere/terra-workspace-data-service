package org.databiosphere.workspacedataservice.shared.model;

import org.databiosphere.workspacedataservice.service.RelationUtils;

public record RelationAttribute(RecordType targetType, String targetId) {

  @Override
  public String toString() {
    return RelationUtils.createRelationString(targetType, targetId);
  }
}
