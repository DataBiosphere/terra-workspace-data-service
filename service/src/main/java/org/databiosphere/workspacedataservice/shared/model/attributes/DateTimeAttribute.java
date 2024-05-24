package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.time.LocalDateTime;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class DateTimeAttribute extends ScalarAttribute<LocalDateTime> {
  DateTimeAttribute(LocalDateTime value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.DATE_TIME;
  }
}
