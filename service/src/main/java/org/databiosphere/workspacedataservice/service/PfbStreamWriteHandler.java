package org.databiosphere.workspacedataservice.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public class PfbStreamWriteHandler implements StreamingWriteHandler {

  private final Stream<GenericRecord> inputStream;

  public PfbStreamWriteHandler(Stream<GenericRecord> inputStream) {
    this.inputStream = inputStream;
  }

  public WriteStreamInfo readRecords(int numRecords) throws IOException {

    Map<RecordType, List<Record>> records =
        inputStream
            .map(this::genericRecordToRecord)
            .collect(Collectors.groupingBy(rec -> rec.getRecordType()));

    // TODO return multiple WriteStreamInfos?  WHAT TO DO

    return new WriteStreamInfo(records., OperationType.UPSERT);
  }

  private org.databiosphere.workspacedataservice.shared.model.Record genericRecordToRecord(
      GenericRecord genRec) {
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
      attributes.putAttribute(field.name(), objectAttributes.get(field.name()));
    }
    converted.setAttributes(attributes);
    return converted;
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
