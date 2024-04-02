package org.databiosphere.workspacedataservice.recordsink;

import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;

public interface RecordSinkFactory {
  RecordSink buildRecordSink(ImportDetails importDetails);

  RecordSink buildRecordSink(CollectionId collectionId);
}
