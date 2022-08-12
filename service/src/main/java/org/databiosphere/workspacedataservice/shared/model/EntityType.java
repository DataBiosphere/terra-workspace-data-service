package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

public class EntityType {

  public EntityType(String name) {
    this.name = name;
  }

  public EntityType() {}

  private String name;

  @JsonValue
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "EntityType{" + "name='" + name + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityType that = (EntityType) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
