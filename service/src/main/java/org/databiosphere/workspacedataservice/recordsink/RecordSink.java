package org.databiosphere.workspacedataservice.recordsink;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * Implementations of this interface are responsible for modifying the schema and writing/deleting
 * batches of records.
 */
public interface RecordSink {
  /** Create or modify the schema for a record type and write the records. */
  Map<String, DataTypeMapping> createOrModifyRecordType(
      UUID collectionId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String recordTypePrimaryKey);

  /** Perform the given {@link OperationType} on the batch of records. */
  void writeBatch(
      UUID collectionId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      OperationType opType,
      List<Record> records,
      String primaryKey)
      throws BatchWriteException, IOException;
}
