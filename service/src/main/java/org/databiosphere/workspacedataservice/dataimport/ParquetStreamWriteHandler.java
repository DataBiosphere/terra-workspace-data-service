package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.databiosphere.workspacedataservice.service.TwoPassStreamingWriteHandler;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

// TODO AJ-1519 reorganize StreamingWriteHandler out of the service package, and add some
//     hierarchy to the dataimport package,
public class ParquetStreamWriteHandler implements TwoPassStreamingWriteHandler {

  private final ParquetReader<GenericRecord> parquetReader;
  private final ImportMode importMode;

  private final TdrManifestImportTable table;
  private final ObjectMapper objectMapper;

  public ParquetStreamWriteHandler(
      ParquetReader<GenericRecord> parquetReader,
      ImportMode importMode,
      TdrManifestImportTable table,
      ObjectMapper objectMapper) {
    this.parquetReader = parquetReader;
    this.importMode = importMode;
    this.table = table;
    this.objectMapper = objectMapper;
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
    ParquetRecordConverter converter = new ParquetRecordConverter(table, objectMapper);
    List<Record> records =
        genericRecords.stream().map(gr -> converter.genericRecordToRecord(gr, importMode)).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    parquetReader.close();
  }
}
