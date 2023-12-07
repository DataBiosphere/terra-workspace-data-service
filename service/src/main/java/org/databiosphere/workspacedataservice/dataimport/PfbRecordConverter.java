package org.databiosphere.workspacedataservice.dataimport;

import static bio.terra.pfb.PfbReader.convertEnum;
import static org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler.PfbImportMode.RELATIONS;

import com.google.mu.util.stream.BiStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PfbRecordConverter.class);

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";
  public static final String RELATIONS_FIELD = "relations";
  public static final String RELATIONS_ID = "dst_id";
  public static final String RELATIONS_NAME = "dst_name";

  public Record genericRecordToRecord(
      GenericRecord genRec, PfbStreamWriteHandler.PfbImportMode pfbImportMode) {
    // create the WDS record shell (id, record type, empty attributes)
    Record converted =
        new Record(
            genRec.get(ID_FIELD).toString(),
            RecordType.valueOf(genRec.get(TYPE_FIELD).toString()),
            RecordAttributes.empty());
    if (pfbImportMode == RELATIONS) {
      return addRelations(genRec, converted);
    } else {
      return addAttributes(genRec, converted);
    }
  }

  private Record addAttributes(GenericRecord genRec, Record converted) {
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

  private Record addRelations(GenericRecord genRec, Record converted) {
    // get the relations array from the record
    if (genRec.get(RELATIONS_FIELD) instanceof Collection<?> relationArray
        && !relationArray.isEmpty()) {
      RecordAttributes attributes = RecordAttributes.empty();
      for (Object relationObject : relationArray) {
        // Here we assume that the relations object is a GenericRecord with keys "dst_name" and
        // "dst_id"
        if (relationObject instanceof GenericRecord relation) {
          String relationType = relation.get(RELATIONS_NAME).toString();
          String relationId = relation.get(RELATIONS_ID).toString();
          // Give the relation column the name of the record type it's linked to
          attributes.putAttribute(
              relationType,
              RelationUtils.createRelationString(RecordType.valueOf(relationType), relationId));
        }
      }
      converted.setAttributes(attributes);
    }
    return converted;
  }

  Object convertAttributeType(Object attribute) {

    if (attribute == null) {
      return null;
    }

    // For list of Avro types - see
    // https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/package-summary.html#package_description

    // Avro records
    if (attribute instanceof GenericRecord recordAttr) {
      return recordAttr.toString(); // TODO AJ-1478: Make these into WDS json?
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

    // Avro maps
    if (attribute instanceof Map<?, ?> mapAttr) {
      // recurse
      return BiStream.from(mapAttr)
          .mapKeys(Object::toString)
          .mapValues(this::convertAttributeType)
          .toMap();
    }

    // Avro fixed
    if (attribute instanceof GenericFixed fixedAttr) {
      return new String(fixedAttr.bytes());
    }

    // Avro strings
    if (attribute instanceof String stringAttr) {
      return stringAttr;
    }

    // Avro bytes
    if (attribute instanceof ByteBuffer byteBufferAttr) {
      return new String(byteBufferAttr.array());
    }

    // Avro ints
    if (attribute instanceof Integer intAttr) {
      return BigDecimal.valueOf(intAttr);
    }

    // Avro longs
    if (attribute instanceof Long longAttr) {
      return BigDecimal.valueOf(longAttr);
    }

    // Avro floats
    if (attribute instanceof Float floatAttr) {
      return BigDecimal.valueOf(floatAttr);
    }

    // Avro doubles
    if (attribute instanceof Double doubleAttr) {
      return BigDecimal.valueOf(doubleAttr);
    }

    // Avro booleans
    if (attribute instanceof Boolean boolAttr) {
      return boolAttr;
    }

    LOGGER.warn(
        "convertAttributeType received value \"{}\" with unexpected type {}",
        attribute,
        attribute.getClass());
    return attribute.toString();
  }
}
