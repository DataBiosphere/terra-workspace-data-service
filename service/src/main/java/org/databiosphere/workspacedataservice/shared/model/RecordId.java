package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

public class RecordId {

  private String entityIdentifier;

  @JsonCreator
  public RecordId(String entityIdentifier) {
    this.entityIdentifier = entityIdentifier;
  }

  @JsonValue
  public String getEntityIdentifier() {
    return entityIdentifier;
  }

  @Override
  public String toString() {
    return "EntityId{" + "entityIdentifier='" + entityIdentifier + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RecordId recordId = (RecordId) o;
    return Objects.equals(entityIdentifier, recordId.entityIdentifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityIdentifier);
  }
}
