package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class BooleanArrayAttribute extends ArrayAttribute<BooleanAttribute> {
  BooleanArrayAttribute(List<BooleanAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_BOOLEAN;
  }

  @Override
  public Boolean[] sqlValue() {
    return this.value.stream().map(BooleanAttribute::sqlValue).toArray(Boolean[]::new);
  }
}
