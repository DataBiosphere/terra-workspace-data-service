package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler;
import org.databiosphere.workspacedataservice.service.StreamingWriteHandler;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

public class ParquetStreamWriteHandler implements StreamingWriteHandler {

  private final ParquetReader<GenericRecord> parquetReader;
  private final PfbStreamWriteHandler.PfbImportMode pfbImportMode;

  private final TdrManifestImportTable table;
  private final ObjectMapper objectMapper;

  public ParquetStreamWriteHandler(
      ParquetReader<GenericRecord> parquetReader,
      PfbStreamWriteHandler.PfbImportMode pfbImportMode,
      TdrManifestImportTable table,
      ObjectMapper objectMapper) {
    this.parquetReader = parquetReader;
    this.pfbImportMode = pfbImportMode;
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
