package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler;
import org.databiosphere.workspacedataservice.service.StreamingWriteHandler;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

// TODO AJ-1013: how much can be consolidated with PfbStreamWriteHandler?
public class ParquetStreamWriteHandler implements StreamingWriteHandler {

  private final ParquetReader<GenericRecord> parquetReader;
  private final PfbStreamWriteHandler.PfbImportMode pfbImportMode;

  public ParquetStreamWriteHandler(
      ParquetReader<GenericRecord> parquetReader,
      PfbStreamWriteHandler.PfbImportMode pfbImportMode) {
    this.parquetReader = parquetReader;
    this.pfbImportMode = pfbImportMode;
  }

  @Override
  public WriteStreamInfo readRecords(int numRecords) throws IOException {
    List<GenericRecord> genericRecords = new ArrayList<>();

    while (genericRecords.size() < numRecords) {
      GenericRecord genericRecord = parquetReader.read();
      if (genericRecord == null) {
        // end of the parquet input
        break;
      }
      genericRecords.add(genericRecord);
    }

    // convert avro generic records to WDS records
    PfbRecordConverter converter = new PfbRecordConverter();
    List<Record> records =
        genericRecords.stream()
            .map(gr -> converter.genericRecordToRecord(gr, BASE_ATTRIBUTES))
            .toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    parquetReader.close();
  }
}
