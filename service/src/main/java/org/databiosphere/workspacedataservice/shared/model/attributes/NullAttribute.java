package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.Objects;

public class NullAttribute extends ScalarAttribute<Object> {

  public static final NullAttribute INSTANCE = new NullAttribute();

  private NullAttribute() {
    super("NullAttribute");
  }

  @Override
  public String toString() {
    return "NullAttribute";
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof NullAttribute;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(null);
  }

  @Override
  public Object sqlValue() {
    return null;
  }

  @Override
  public Object getValue() {
    // don't return an actual null; return this wrapper
    return null;
  }
}
