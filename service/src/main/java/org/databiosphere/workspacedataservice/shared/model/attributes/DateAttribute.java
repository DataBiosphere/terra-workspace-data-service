package org.databiosphere.workspacedataservice.shared.model.attributes;

import java.time.LocalDate;

public class DateAttribute extends ScalarAttribute<LocalDate> {
  DateAttribute(LocalDate value) {
    super(value);
  }
}
