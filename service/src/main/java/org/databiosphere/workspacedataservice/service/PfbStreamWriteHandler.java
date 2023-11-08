package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
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

    // TODO AJ-1227: don't re-create the Stream<GenericRecord> for each batch. We should be able to
    //    create the Stream<GenericRecord> once in the constructor, then pull numRecords records
    //    from it in each batch.
    // TODO AJ-1227: make sure this has a unit test that involves multiple batches
    // translate the Avro DataFileStream into a Java stream
    Stream<GenericRecord> recordStream =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(inputStream.iterator(), Spliterator.ORDERED),
                false)
            .limit(numRecords);

    List<Record> records = recordStream.map(this::genericRecordToRecord).toList();

    return new WriteStreamInfo(records, OperationType.UPSERT);
  }

  // TODO AJ-1227: move to a separate, unit-testable, class
  private Record genericRecordToRecord(GenericRecord genRec) {
    Record converted =
        new Record(
            genRec.get("id").toString(),
            RecordType.valueOf(genRec.get("name").toString()),
            RecordAttributes.empty());
    GenericRecord objectAttributes = (GenericRecord) genRec.get("object"); // contains attributes
    Schema schema = objectAttributes.getSchema();
    List<Schema.Field> fields = schema.getFields();
    RecordAttributes attributes = RecordAttributes.empty();
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object value =
          objectAttributes.get(fieldName) == null
              ? null
              : convertAttributeType(objectAttributes.get(fieldName));
      attributes.putAttribute(fieldName, value);
    }
    converted.setAttributes(attributes);
    return converted;
  }

  // TODO AJ-1227: move to a separate, unit-testable, class
  private Object convertAttributeType(Object attribute) {
    if (attribute == null) {
      return null;
    }
    if (attribute instanceof Long /*or other number*/) {
      return attribute;
    }
    return attribute.toString(); // easier for the datatype inferer to parse
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
