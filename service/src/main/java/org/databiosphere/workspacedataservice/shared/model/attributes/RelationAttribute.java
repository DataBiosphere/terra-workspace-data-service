package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.RelationTarget;

/** This scalar Attribute is a relation from one WDS Record to another. */
public class RelationAttribute extends ScalarAttribute<RelationTarget> {

  public RelationAttribute(RecordType targetType, String targetId) {
    super(new RelationTarget(targetType, targetId));
  }

  public RecordType getTargetType() {
    return value.targetType();
  }

  public String getTargetId() {
    return value.targetId();
  }

  @Override
  public String toString() {
    return RelationUtils.createRelationString(value.targetType(), value.targetId());
  }
}
