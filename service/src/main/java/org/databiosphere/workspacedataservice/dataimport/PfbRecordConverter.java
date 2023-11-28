package org.databiosphere.workspacedataservice.dataimport;

import static bio.terra.pfb.PfbReader.convertEnum;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
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

  Object convertAttributeType(Object attribute) {

    if (attribute == null) {
      return null;
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

    // Avro enums
    if (attribute instanceof GenericEnumSymbol<?> enumAttr) {
      return convertEnum(enumAttr.toString());
    }

    // Avro arrays
    if (attribute instanceof Collection<?> collAttr) {
      // recurse
      return collAttr.stream().map(this::convertAttributeType).toList();
    }

    // TODO AJ-1478: handle remaining possible Avro datatypes:
    //     Avro bytes are implemented as ByteBuffer. toString() these?
    //     Avro fixed are implemented as GenericFixed. toString() these?
    //     Avro maps are implemented as Map. Can we make these into WDS json?
    //     Avro records are implemented as GenericRecord. Can we make these into WDS json?

    // for now, everything else is a String
    return attribute.toString();
  }
}
