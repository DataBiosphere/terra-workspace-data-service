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

public class PfbStreamWriteHandler implements TwoPassStreamingWriteHandler {
  public enum PfbImportMode {
    RELATIONS,
    BASE_ATTRIBUTES
  }

  private final DataFileStream<GenericRecord> inputStream;
  private final PfbImportMode pfbImportMode;
  private final PfbRecordConverter pfbRecordConverter;

  /**
   * Create a new PfbStreamWriteHandler and specify the expected schemas for the PFB.
   *
   * @param inputStream the PFB stream
   * @param pfbImportMode the mode to use when importing the PFB
   * @param pfbRecordConverter the converter to use when converting PFB records to WDS records
   */
  public PfbStreamWriteHandler(
      DataFileStream<GenericRecord> inputStream,
      PfbImportMode pfbImportMode,
      PfbRecordConverter pfbRecordConverter) {
    this.inputStream = inputStream;
    this.pfbImportMode = pfbImportMode;
    this.pfbRecordConverter = pfbRecordConverter;
  }

  public WriteStreamInfo readRecords(int numRecords) {
    // pull the next `numRecords` rows from the inputStream and translate to a Java Stream
    Stream<GenericRecord> pfbBatch =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(inputStream, Spliterator.ORDERED), false)
            .limit(numRecords);

    // convert the PFB GenericRecord objects into WDS Record objects
    List<Record> records =
        pfbBatch.map(rec -> pfbRecordConverter.genericRecordToRecord(rec, pfbImportMode)).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
