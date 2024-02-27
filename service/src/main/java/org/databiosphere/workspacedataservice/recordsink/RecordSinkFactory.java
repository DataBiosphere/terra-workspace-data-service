package org.databiosphere.workspacedataservice.recordsink;

import java.util.UUID;

public interface RecordSinkFactory {
  RecordSink buildRecordSink(UUID collectionId, String prefix);
}
