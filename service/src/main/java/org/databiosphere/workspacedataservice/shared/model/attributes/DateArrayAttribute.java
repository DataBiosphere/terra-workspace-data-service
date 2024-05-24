package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class DateArrayAttribute extends ArrayAttribute<DateAttribute> {
  DateArrayAttribute(List<DateAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_DATE;
  }
}
