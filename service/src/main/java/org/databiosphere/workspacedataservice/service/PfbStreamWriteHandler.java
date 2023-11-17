package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

public class PfbStreamWriteHandler implements StreamingWriteHandler {

  private final DataFileStream<GenericRecord> inputStream;
  private final Map<String, Map<String, DataTypeMapping>> recordTypeSchemas;

  /**
   * Create a new PfbStreamWriteHandler and specify the expected schemas for the PFB.
   *
   * @param inputStream the PFB stream
   * @param recordTypeSchemas the expected WDS schema for the PFB
   */
  public PfbStreamWriteHandler(
      DataFileStream<GenericRecord> inputStream,
      Map<String, Map<String, DataTypeMapping>> recordTypeSchemas) {
    this.inputStream = inputStream;
    this.recordTypeSchemas = recordTypeSchemas;
  }

  public WriteStreamInfo readRecords(int numRecords) throws IOException {
    // pull the next `numRecords` rows from the inputStream and translate to a Java Stream
    Stream<GenericRecord> pfbBatch =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(inputStream, Spliterator.ORDERED), false)
            .limit(numRecords);

    // convert the PFB GenericRecord objects into WDS Record objects
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter(recordTypeSchemas);
    List<Record> records = pfbBatch.map(pfbRecordConverter::genericRecordToRecord).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
