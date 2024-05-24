package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class StringArrayAttribute extends ArrayAttribute<StringAttribute> {
  StringArrayAttribute(List<StringAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_STRING;
  }
}
