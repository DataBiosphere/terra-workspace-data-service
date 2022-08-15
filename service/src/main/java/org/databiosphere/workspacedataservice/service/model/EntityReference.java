package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.EntityType;

public class EntityReference {

  private final String referenceColName;

  private final EntityType referencedEntityType;

  public EntityReference(String referenceColName, EntityType referencedEntityType) {
    this.referenceColName = referenceColName;
    this.referencedEntityType = referencedEntityType;
  }

  public String getReferenceColName() {
    return referenceColName;
  }

  public EntityType getReferencedEntityType() {
    return referencedEntityType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof EntityReference that)) return false;

    if (!getReferenceColName().equals(that.getReferenceColName())) return false;
    return getReferencedEntityType().equals(that.getReferencedEntityType());
  }

  @Override
  public int hashCode() {
    int result = getReferenceColName().hashCode();
    result = 31 * result + getReferencedEntityType().hashCode();
    return result;
  }
}
