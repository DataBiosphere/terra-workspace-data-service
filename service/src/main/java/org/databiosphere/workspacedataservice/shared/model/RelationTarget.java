package org.databiosphere.workspacedataservice.shared.model;

import org.databiosphere.workspacedataservice.service.RelationUtils;

/**
 * A relation from one WDS record to another.
 *
 * @param targetType
 * @param targetId
 */
public record RelationTarget(RecordType targetType, String targetId) {
  @Override
  public String toString() {
    return RelationUtils.createRelationString(targetType, targetId);
  }
}
