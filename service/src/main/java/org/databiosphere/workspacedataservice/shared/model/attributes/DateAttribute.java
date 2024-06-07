package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.time.LocalDate;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class DateAttribute extends ScalarAttribute<LocalDate> {
  DateAttribute(LocalDate value) {
    super(value);
  }

  @Override
  public LocalDate sqlValue() {
    return this.value;
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.DATE;
  }
}
