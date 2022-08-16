package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

public class RecordType {

  public RecordType(String name) {
    this.name = name;
  }

  public RecordType() {}

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
    RecordType that = (RecordType) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
