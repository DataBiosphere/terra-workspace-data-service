package org.databiosphere.workspacedataservice.recordsource;

import java.io.IOException;
import java.util.function.UnaryOperator;
import org.databiosphere.workspacedataservice.shared.model.Record;

/** Wraps a RecordSource and transforms Records from the wrapped source. */
public class MappedRecordSource implements RecordSource {
  private final RecordSource recordSource;
  private final UnaryOperator<Record> mapRecord;

  public MappedRecordSource(RecordSource recordSource, UnaryOperator<Record> mapRecord) {
    this.recordSource = recordSource;
    this.mapRecord = mapRecord;
  }

  @Override
  public WriteStreamInfo readRecords(int numRecords) throws IOException {
    WriteStreamInfo info = recordSource.readRecords(numRecords);
    return new WriteStreamInfo(
        info.records().stream().map(mapRecord::apply).toList(), info.operationType());
  }

  @Override
  public ImportMode importMode() {
    return recordSource.importMode();
  }

  @Override
  public void close() throws IOException {
    recordSource.close();
  }
}
