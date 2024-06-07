package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class StringArrayAttribute extends ArrayAttribute<StringAttribute> {
  StringArrayAttribute(List<StringAttribute> value) {
    super(value);
  }

  @Override
  public String[] sqlValue() {
    return this.value.stream().map(StringAttribute::sqlValue).toArray(String[]::new);
  }

  @Override
  public List<String> getValue() {
    return this.value.stream().map(StringAttribute::getValue).toList();
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_STRING;
  }
}
