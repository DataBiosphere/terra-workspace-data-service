package org.databiosphere.workspacedataservice.recordsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.stereotype.Component;

@Component
public class RecordSourceFactory {

  private final ObjectMapper objectMapper;
  private final ObjectReader objectReader;

  public RecordSourceFactory(ObjectMapper objectMapper, ObjectReader objectReader) {
    this.objectMapper = objectMapper;
    this.objectReader = objectReader;
  }

  public RecordSource forJson(InputStream inputStream) throws IOException {
    return new JsonRecordSource(inputStream, objectMapper);
  }

  // TsvRecordSource plays a role in primary key resolution, and so we return this
  // particular subclass of RecordSource so the callsite can use this extra
  // method on its interface if needed.
  public TsvRecordSource forTsv(
      InputStream inputStream, RecordType recordType, Optional<String> primaryKey) {
    return new TsvRecordSource(inputStream, objectReader, recordType, primaryKey);
  }

  public RecordSource forTdrImport(
      ParquetReader<GenericRecord> parquetReader,
      TdrManifestImportTable table,
      ImportMode importMode) {
    return new ParquetRecordSource(parquetReader, importMode, table, objectMapper);
  }

  public RecordSource forPfb(DataFileStream<GenericRecord> inputStream, ImportMode importMode) {
    return new PfbRecordSource(inputStream, importMode, objectMapper);
  }
}
