package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class NumberArrayAttribute extends ArrayAttribute<NumberAttribute> {
  NumberArrayAttribute(List<NumberAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_NUMBER;
  }

  @Override
  public Number[] sqlValue() {
    return this.value.stream().map(NumberAttribute::getValue).toArray(Number[]::new);
  }
}
