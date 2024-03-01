package org.databiosphere.workspacedataservice.recordsink;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dataimport.GcpImportDestinationDetails;

public interface RecordSinkFactory {
  RecordSink buildRecordSink(
      UUID collectionId, String prefix, GcpImportDestinationDetails importDestinationDetails);
}
