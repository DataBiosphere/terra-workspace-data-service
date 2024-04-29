package org.databiosphere.workspacedataservice.shared.model.attributes;

/** A scalar attribute - that is, an attribute that has a single value. */
public abstract class ScalarAttribute<T> implements Attribute {
  final T value;

  ScalarAttribute(T value) {
    this.value = value;
  }

  public T getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ScalarAttribute<?> other) {
      return this.value.equals(other.value);
    }
    return false;
  }

  @Override
  public String toString() {
    return this.value.toString();
  }
}
