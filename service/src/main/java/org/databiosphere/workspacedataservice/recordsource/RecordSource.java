package org.databiosphere.workspacedataservice.recordsource;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

public interface RecordSource extends Closeable {

  /**
   * Reads numRecords from the stream unless the operation type changes during the stream in which
   * case we return early and keep the last record read in memory, so it can be returned in a
   * subsequent call.
   *
   * @param numRecords max number of records to read
   * @return info about the records that were read
   * @throws IOException on error
   */
  WriteStreamInfo readRecords(int numRecords) throws IOException;

  /**
   * Enum for use by record sources that need their data imported in two passes. The first pass will
   * upsert base attributes, and the second pass will upsert relation attributes. This allows for
   * data streams where earlier records contain relations to later records.
   */
  enum ImportMode {
    RELATIONS,
    BASE_ATTRIBUTES
  }

  record WriteStreamInfo(List<Record> records, OperationType operationType) {}

  default ImportMode importMode() {
    return ImportMode.BASE_ATTRIBUTES;
  }
}
