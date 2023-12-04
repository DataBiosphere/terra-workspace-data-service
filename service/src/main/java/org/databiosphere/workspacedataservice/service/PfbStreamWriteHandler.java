package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

public class PfbStreamWriteHandler implements StreamingWriteHandler {

  public enum PfbImportMode {
    RELATIONS,
    BASE_ATTRIBUTES
  }

  private final DataFileStream<GenericRecord> inputStream;
  private final PfbImportMode pfbImportMode;

  /**
   * Create a new PfbStreamWriteHandler and specify the expected schemas for the PFB.
   *
   * @param inputStream the PFB stream
   */
  public PfbStreamWriteHandler(
      DataFileStream<GenericRecord> inputStream, PfbImportMode pfbImportMode) {
    this.inputStream = inputStream;
    this.pfbImportMode = pfbImportMode;
  }

  // TODO maybe use a functional interface or something instead of a boolean

  public WriteStreamInfo readRecords(int numRecords) {
    // pull the next `numRecords` rows from the inputStream and translate to a Java Stream
    Stream<GenericRecord> pfbBatch =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(inputStream, Spliterator.ORDERED), false)
            .limit(numRecords);

    // convert the PFB GenericRecord objects into WDS Record objects
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();
    List<Record> records =
        pfbBatch.map(rec -> pfbRecordConverter.genericRecordToRecord(rec, pfbImportMode)).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
