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

    // contains attributes
    if (genRec.get(OBJECT_FIELD) instanceof GenericRecord objectAttributes) {
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
    }

    return converted;
  }

  // TODO AJ-1452: respect the datatypes returned by the PFB. For now, we make no guarantee that
  //    about datatypes; many values are just toString()-ed. This allows us to commit incremental
  //    progress and save some complicated work for later.
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
