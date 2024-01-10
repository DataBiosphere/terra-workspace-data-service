package org.databiosphere.workspacedataservice.recordstream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.databiosphere.workspacedataservice.shared.model.BatchOperation;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

/**
 * Stream-reads inbound json using a JsonParser; returns a WriteStreamInfo containing a batch of
 * Records.
 */
public class JsonStreamWriteHandler implements StreamingWriteHandler {

  private final JsonParser parser;

  private BatchOperation savedRecord;

  private final InputStream inputStream;

  public JsonStreamWriteHandler(InputStream inputStream, ObjectMapper objectMapper)
      throws IOException {
    this.inputStream = inputStream;
    parser = objectMapper.copy().tokenStreamFactory().createParser(inputStream);
    if (parser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalArgumentException("Expected content to be an array");
    }
  }

  /**
   * Reads numRecords from the stream unless the operation type changes during the stream in which
   * case we return early and keep the last record read in memory so it can be returned in a
   * subsequent call.
   *
   * @param numRecords max number of records to read
   * @return info about the records that were read
   * @throws IOException on error
   */
  public WriteStreamInfo readRecords(int numRecords) throws IOException {
    int recordsProcessed = 0;
    List<Record> result = new ArrayList<>(numRecords);
    OperationType lastOp = null;
    if (savedRecord != null) {
      result.add(savedRecord.getRecord());
      lastOp = savedRecord.getOperation();
      savedRecord = null;
    }
    // order matters in this condition, we don't want to advance the parser (call
    // nextToken()) unless we're
    // ready to consume the next BatchOperation
    while (recordsProcessed < numRecords
        && parser.nextToken() != JsonToken.END_ARRAY
        && parser.hasCurrentToken()) {
      BatchOperation op = parser.readValueAs(BatchOperation.class);
      OperationType opType = op.getOperation();
      if (lastOp != null && lastOp != opType) {
        savedRecord = op;
        return new WriteStreamInfo(result, lastOp);
      }
      recordsProcessed++;
      result.add(op.getRecord());
      lastOp = opType;
    }
    return new WriteStreamInfo(result, lastOp);
  }

  @Override
  public void close() throws IOException {
    parser.close();
    inputStream.close();
  }

  // Exposed to assist with unit tests.
  public JsonParser getParser() {
    return this.parser;
  }
}
