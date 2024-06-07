package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.time.LocalDateTime;
import java.util.List;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class DateTimeArrayAttribute extends ArrayAttribute<DateTimeAttribute> {
  DateTimeArrayAttribute(List<DateTimeAttribute> value) {
    super(value);
  }

  @Override
  public DataTypeMapping getDataTypeMapping() {
    return DataTypeMapping.ARRAY_OF_DATE_TIME;
  }

  @Override
  public LocalDateTime[] sqlValue() {
    return this.value.stream().map(DateTimeAttribute::sqlValue).toArray(LocalDateTime[]::new);
  }

  @Override
  public List<LocalDateTime> getValue() {
    return this.value.stream().map(DateTimeAttribute::getValue).toList();
  }
}
