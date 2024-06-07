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
   * Callback always invoked at the end of a series of batch operations. This should execute any
   * code to run on both success and failure, such as closing open files.
   *
   * @throws DataImportException if an error occurs while closing the sink
   */
  default void close() throws DataImportException {
    // no-op
  }

  /**
   * Method manually invoked by callers to indicate success of all batch operations. This should
   * execute any code to run only on success, such as sending events to other systems.
   *
   * @throws DataImportException if an error occurs while closing the sink
   */
  void success() throws DataImportException;
}
