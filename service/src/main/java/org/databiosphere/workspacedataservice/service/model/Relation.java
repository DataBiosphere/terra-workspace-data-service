package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.RecordType;

public record Relation(String referenceColName, RecordType referencedRecordType) {

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Relation that)) return false;

    if (!referenceColName().equals(that.referenceColName())) return false;
    return referencedRecordType().equals(that.referencedRecordType());
  }

  @Override
  public int hashCode() {
    int result = referenceColName().hashCode();
    result = 31 * result + referencedRecordType().hashCode();
    return result;
  }
}
