package org.databiosphere.workspacedataservice.recordsink;

import org.databiosphere.workspacedataservice.dataimport.ImportDetails;

public interface RecordSinkFactory {
  RecordSink buildRecordSink(ImportDetails importDetails);
}
