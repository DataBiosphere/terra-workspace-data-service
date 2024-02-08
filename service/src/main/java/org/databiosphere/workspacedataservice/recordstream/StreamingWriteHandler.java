package org.databiosphere.workspacedataservice.recordstream;

import java.io.Closeable;
import java.io.IOException;
import org.databiosphere.workspacedataservice.service.BatchWriteService.WriteStreamInfo;

public interface StreamingWriteHandler extends Closeable {

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
}
