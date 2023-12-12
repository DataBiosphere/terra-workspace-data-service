package org.databiosphere.workspacedataservice.dataimport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler;
import org.databiosphere.workspacedataservice.service.StreamingWriteHandler;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public class ParquetStreamWriteHandler implements StreamingWriteHandler {

  private final ParquetReader<GenericRecord> parquetReader;
  private final PfbStreamWriteHandler.PfbImportMode pfbImportMode;

  private final RecordType recordType;
  private final String primaryKey;

  public ParquetStreamWriteHandler(
      ParquetReader<GenericRecord> parquetReader,
      PfbStreamWriteHandler.PfbImportMode pfbImportMode,
      RecordType recordType,
      String primaryKey) {
    this.parquetReader = parquetReader;
    this.pfbImportMode = pfbImportMode;
    this.recordType = recordType;
    this.primaryKey = primaryKey;
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
    ParquetRecordConverter converter = new ParquetRecordConverter(recordType, primaryKey);
    List<Record> records =
        genericRecords.stream()
            .map(gr -> converter.genericRecordToRecord(gr, pfbImportMode))
            .toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    parquetReader.close();
  }
}
