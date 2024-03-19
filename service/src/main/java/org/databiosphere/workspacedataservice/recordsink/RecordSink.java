package org.databiosphere.workspacedataservice.recordsink;

import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * Implementations of this interface are responsible for modifying the schema and writing/deleting
 * batches of records.
 */
public interface RecordSink extends AutoCloseable {
  /**
   * Create or modify the schema for a record type and write the records.
   *
   * @throws DataImportException if an error occurs creating or modifying types
   */
  Map<String, DataTypeMapping> createOrModifyRecordType(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String recordTypePrimaryKey)
      throws DataImportException;

  /**
   * Upsert the given batch of records.
   *
   * @throws DataImportException if an error occurs while upserting the records
   */
  void upsertBatch(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String primaryKey)
      throws DataImportException;

  /**
   * Delete the given batch of records.
   *
   * @throws DataImportException if an error occurs while deleting the records
   */
  void deleteBatch(RecordType recordType, List<Record> records) throws DataImportException;

  /**
   * Callback invoked at the end of a series of batches operations with the result. Implementers can
   * commit changes, clean up resources, publish results, etc.
   *
   * @throws DataImportException if an error occurs while closing the sink
   */
  void close() throws DataImportException;
}
