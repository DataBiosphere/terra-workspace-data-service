package org.databiosphere.workspacedataservice.recordsink;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.ImportDestinationDetails;

public interface RecordSinkFactory {
  RecordSink buildRecordSink(
      UUID collectionId, String prefix, ImportDestinationDetails importDestinationDetails);
}
