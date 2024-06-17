package org.databiosphere.workspacedataservice.shared.model.attributes;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class StringAttribute extends ScalarAttribute<String> {
  StringAttribute(String value) {
    super(value);
  }

  @Override
  public String sqlValue() {
    return this.value;
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.STRING;
  }
}
