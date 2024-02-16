package org.databiosphere.workspacedataservice.recordsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

public class PfbRecordSource implements TwoPassRecordSource {

  private final DataFileStream<GenericRecord> inputStream;
  private final ImportMode importMode;
  private final ObjectMapper objectMapper;

  /**
   * Create a new PfbRecordSource and specify the expected schemas for the PFB.
   *
   * @param inputStream the PFB stream
   * @param importMode the mode to use when importing the PFB
   * @param objectMapper the object mapper to use when converting PFB records to WDS records
   */
  public PfbRecordSource(
      DataFileStream<GenericRecord> inputStream, ImportMode importMode, ObjectMapper objectMapper) {
    this.inputStream = inputStream;
    this.importMode = importMode;
    this.objectMapper = objectMapper;
  }

  public WriteStreamInfo readRecords(int numRecords) {
    // pull the next `numRecords` rows from the inputStream and translate to a Java Stream
    Stream<GenericRecord> pfbBatch =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(inputStream, Spliterator.ORDERED), false)
            .limit(numRecords);

    // convert the PFB GenericRecord objects into WDS Record objects
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter(objectMapper);
    List<Record> records =
        pfbBatch.map(rec -> pfbRecordConverter.convert(rec, importMode)).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
