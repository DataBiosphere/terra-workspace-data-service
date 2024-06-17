package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;

/**
 * An array attribute - that is, an attribute that can have multiple values, all of which are the
 * same datatype.
 */

// TODO AJ-494 how to parameterize ScalarAttribute<?>

public abstract class ArrayAttribute<T extends ScalarAttribute<?>> implements Attribute {

  final List<T> value;

  ArrayAttribute(List<T> value) {
    this.value = value;
  }
}
