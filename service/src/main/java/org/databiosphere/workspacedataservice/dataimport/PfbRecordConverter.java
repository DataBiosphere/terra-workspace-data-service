package org.databiosphere.workspacedataservice.dataimport;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter {

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";

  private final Map<String, Map<String, DataTypeMapping>> recordTypeSchemas;

  public PfbRecordConverter(Map<String, Map<String, DataTypeMapping>> recordTypeSchemas) {
    this.recordTypeSchemas = recordTypeSchemas;
  }

  public Record genericRecordToRecord(GenericRecord genRec) {
    // create the WDS record shell (id, record type, empty attributes)
    Record converted =
        new Record(
            genRec.get(ID_FIELD).toString(),
            RecordType.valueOf(genRec.get(TYPE_FIELD).toString()),
            RecordAttributes.empty());

    // retrieve the expected schema for this record type
    Map<String, DataTypeMapping> wdsTypeSchema =
        recordTypeSchemas.getOrDefault(genRec.get(TYPE_FIELD).toString(), Map.of());

    // loop over all Avro fields and add to the record's attributes
    if (genRec.get(OBJECT_FIELD) instanceof GenericRecord objectAttributes) {
      Schema schema = objectAttributes.getSchema();
      List<Schema.Field> fields = schema.getFields();
      RecordAttributes attributes = RecordAttributes.empty();
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Object value = null;
        if (objectAttributes.get(fieldName) != null) {
          DataTypeMapping targetDataType = wdsTypeSchema.get(fieldName);
          value = convertAttributeType(objectAttributes.get(fieldName), targetDataType);
        }
        attributes.putAttribute(fieldName, value);
      }
      converted.setAttributes(attributes);
    }

    return converted;
  }

  // TODO AJ-1452: This only handles scalar numbers, booleans and strings. We need to add support
  //     for other datatypes later.
  Object convertAttributeType(Object attribute, DataTypeMapping targetDataType) {

    if (attribute == null) {
      return null;
    }
    if (targetDataType == null) {
      return attribute.toString();
    }
    Object returnValue = null;
    switch (targetDataType) {
      case NUMBER:
        // Avro numbers - see
        // https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/package-summary.html#package_description
        if (attribute instanceof Long longAttr) {
          returnValue = BigDecimal.valueOf(longAttr);
        } else if (attribute instanceof Integer intAttr) {
          returnValue = BigDecimal.valueOf(intAttr);
        } else if (attribute instanceof Float floatAttr) {
          returnValue = BigDecimal.valueOf(floatAttr);
        } else if (attribute instanceof Double doubleAttr) {
          returnValue = BigDecimal.valueOf(doubleAttr);
        }
        break;
      case BOOLEAN:
        if (attribute instanceof Boolean boolAttr) {
          returnValue = boolAttr;
        }
        break;
      default:
        returnValue = attribute.toString();
    }
    if (returnValue == null) {
      returnValue = attribute.toString();
    }

    return returnValue;
  }
}
