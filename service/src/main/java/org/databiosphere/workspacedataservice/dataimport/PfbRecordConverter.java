package org.databiosphere.workspacedataservice.dataimport;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter {

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";

  public Record genericRecordToRecord(GenericRecord genRec) {
    Record converted =
        new Record(
            genRec.get(ID_FIELD).toString(),
            RecordType.valueOf(genRec.get(TYPE_FIELD).toString()),
            RecordAttributes.empty());
    GenericRecord objectAttributes =
        (GenericRecord) genRec.get(OBJECT_FIELD); // contains attributes
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

  Object convertAttributeType(Object attribute) {
    if (attribute == null) {
      return null;
    }
    if (attribute instanceof Long /*or other number*/) {
      return attribute;
    }
    return attribute.toString(); // easier for the datatype inferer to parse
  }
}
