package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class NumberAttribute extends ScalarAttribute<Number> {
  NumberAttribute(Number value) {
    super(value);
  }

  @Override
  public Number sqlValue() {
    return this.value;
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.NUMBER;
  }
}
