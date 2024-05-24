package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.time.LocalDateTime;

public class DateTimeAttribute extends ScalarAttribute<LocalDateTime> {
  DateTimeAttribute(LocalDateTime value) {
    super(value);
  }
}
