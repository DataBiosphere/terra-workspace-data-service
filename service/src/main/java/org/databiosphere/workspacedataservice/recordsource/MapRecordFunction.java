package org.databiosphere.workspacedataservice.recordsource;

import org.databiosphere.workspacedataservice.shared.model.Record;

@FunctionalInterface
public interface MapRecordFunction {
  Record apply(Record record);
}
