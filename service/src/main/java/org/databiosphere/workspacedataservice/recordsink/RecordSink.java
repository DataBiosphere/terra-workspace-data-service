package org.databiosphere.workspacedataservice.recordsink;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * Implementations of this interface are responsible for modifying the schema and writing/deleting
 * batches of records.
 */
public interface RecordSink {
  /** Create or modify the schema for a record type and write the records. */
  Map<String, DataTypeMapping> createOrModifyRecordType(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String recordTypePrimaryKey);

  /** Upsert the given batch of records. */
  void upsertBatch(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String primaryKey)
      throws BatchWriteException, IOException;

  /** Delete the given batch of records. */
  void deleteBatch(RecordType recordType, List<Record> records)
      throws BatchWriteException, IOException;

  String getBlobName();
}
