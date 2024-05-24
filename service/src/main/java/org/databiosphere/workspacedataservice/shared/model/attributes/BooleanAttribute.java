package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class BooleanAttribute extends ScalarAttribute<Boolean> {
  BooleanAttribute(Boolean value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.BOOLEAN;
  }
}
