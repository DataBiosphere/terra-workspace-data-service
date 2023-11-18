package org.databiosphere.workspacedataservice.dataimport;

import java.math.BigDecimal;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter {

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public Record genericRecordToRecord(GenericRecord genRec) {
    // create the WDS record shell (id, record type, empty attributes)
    Record converted =
        new Record(
            genRec.get(ID_FIELD).toString(),
            RecordType.valueOf(genRec.get(TYPE_FIELD).toString()),
            RecordAttributes.empty());

    // loop over all Avro fields and add to the record's attributes
    if (genRec.get(OBJECT_FIELD) instanceof GenericRecord objectAttributes) {
      Schema schema = objectAttributes.getSchema();
      List<Schema.Field> fields = schema.getFields();
      RecordAttributes attributes = RecordAttributes.empty();
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Object value = null;
        if (objectAttributes.get(fieldName) != null) {
          // TODO AJ-1452: is this an enum?
          value = convertAttributeType(objectAttributes.get(fieldName), field);
        }
        attributes.putAttribute(fieldName, value);
      }
      converted.setAttributes(attributes);
    }

    return converted;
  }

  // TODO AJ-1452: This only handles scalar numbers, booleans and strings. We need to add support
  //     for other datatypes later.
  Object convertAttributeType(Object attribute, Schema.Field field) {

    if (attribute == null) {
      return null;
    }
    if (field == null) {
      throw new PfbParsingException("Something went wrong. Field is null.");
    }

    // Avro numbers - see
    // https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/package-summary.html#package_description
    if (attribute instanceof Long longAttr) {
      return BigDecimal.valueOf(longAttr);
    }
    if (attribute instanceof Integer intAttr) {
      return BigDecimal.valueOf(intAttr);
    }
    if (attribute instanceof Float floatAttr) {
      return BigDecimal.valueOf(floatAttr);
    }
    if (attribute instanceof Double doubleAttr) {
      return BigDecimal.valueOf(doubleAttr);
    }

    // Avro booleans
    if (attribute instanceof Boolean boolAttr) {
      return boolAttr;
    }

    // for now, everything else is a String
    return attribute.toString();
  }
}
