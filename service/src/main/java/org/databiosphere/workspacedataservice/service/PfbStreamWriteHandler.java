package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

public class PfbStreamWriteHandler implements StreamingWriteHandler {

  private final DataFileStream<GenericRecord> inputStream;

  public PfbStreamWriteHandler(DataFileStream<GenericRecord> inputStream) {
    this.inputStream = inputStream;
  }

  // TODO AJ-1227: make sure this has a unit test that involves multiple batches
  public WriteStreamInfo readRecords(int numRecords) throws IOException {

    // pull up to numRecords rows, as GenericRecord, from the PFB's DataFileStream and materialize
    // into a List
    List<GenericRecord> collector = new ArrayList<>(numRecords);
    int i = 0;
    while (inputStream.hasNext() && i < numRecords) {
      i++;
      collector.add(inputStream.next());
    }

    // convert the PFB GenericRecord objects into WDS Record objects
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();
    List<Record> records =
        collector.stream().map(pfbRecordConverter::genericRecordToRecord).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
