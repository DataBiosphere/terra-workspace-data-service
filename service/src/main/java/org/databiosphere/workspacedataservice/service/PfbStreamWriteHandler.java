package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public class PfbStreamWriteHandler implements StreamingWriteHandler {

  private final DataFileStream<GenericRecord> inputStream;

  public PfbStreamWriteHandler(DataFileStream<GenericRecord> inputStream) {
    this.inputStream = inputStream;
  }

  public WriteStreamInfo readRecords(int numRecords) throws IOException {

    // translate the Avro DataFileStream into a Java stream
    Stream<GenericRecord> recordStream =
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(inputStream.iterator(), Spliterator.ORDERED),
            false);

    // TODO pay attention to numRecords
    List<Record> records =
        recordStream.map(this::genericRecordToRecord).collect(Collectors.toList());

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  // TODO how to not write the whole path
  private org.databiosphere.workspacedataservice.shared.model.Record genericRecordToRecord(
      GenericRecord genRec) {
    // TODO ok to use plain strings here so should they be static variables or what?
    org.databiosphere.workspacedataservice.shared.model.Record converted =
        new org.databiosphere.workspacedataservice.shared.model.Record(
            genRec.get("id").toString(),
            RecordType.valueOf(genRec.get("name").toString()),
            RecordAttributes.empty());
    GenericRecord objectAttributes = (GenericRecord) genRec.get("object"); // contains attributes
    Schema schema = objectAttributes.getSchema();
    List<Schema.Field> fields = schema.getFields();
    RecordAttributes attributes = RecordAttributes.empty();
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      // TODO deal with this better?
      // Making everything a string to avoid weird data type issues later
      String value =
          objectAttributes.get(fieldName) == null
              ? null
              : objectAttributes.get(fieldName).toString();
      attributes.putAttribute(fieldName, value);
    }
    converted.setAttributes(attributes);
    return converted;
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
