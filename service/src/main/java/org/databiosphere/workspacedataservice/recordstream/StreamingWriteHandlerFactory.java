package org.databiosphere.workspacedataservice.recordstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler.ImportMode;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.stereotype.Component;

@Component
public class StreamingWriteHandlerFactory {

  private final ObjectMapper objectMapper;
  private final ObjectReader objectReader;

  public StreamingWriteHandlerFactory(ObjectMapper objectMapper, ObjectReader objectReader) {
    this.objectMapper = objectMapper;
    this.objectReader = objectReader;
  }

  public StreamingWriteHandler forJson(InputStream inputStream) throws IOException {
    return new JsonStreamWriteHandler(inputStream, objectMapper);
  }

  // TsvStreamWriteHandler plays a role in primary key resolution, and so we return this
  // particular subclass of StreamingWriteHandler so the callsite can use this extra
  // method on its interface if needed.
  public TsvStreamWriteHandler forTsv(
      InputStream inputStream, RecordType recordType, Optional<String> primaryKey) {
    return new TsvStreamWriteHandler(inputStream, objectReader, recordType, primaryKey);
  }

  public StreamingWriteHandler forTdrImport(
      ParquetReader<GenericRecord> parquetReader,
      TdrManifestImportTable table,
      ImportMode importMode) {
    return new ParquetStreamWriteHandler(parquetReader, importMode, table, objectMapper);
  }

  public StreamingWriteHandler forPfb(
      DataFileStream<GenericRecord> inputStream, ImportMode importMode) {
    return new PfbStreamWriteHandler(inputStream, importMode, objectMapper);
  }
}
