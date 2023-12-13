package org.databiosphere.workspacedataservice.dataimport;


import java.util.Collection;
import java.util.Set;

import static bio.terra.pfb.PfbReader.convertEnum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;

import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to convert a PFB's GenericRecord to WDS's Record */
public class PfbRecordConverter extends AvroRecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PfbRecordConverter.class);

  public static final String ID_FIELD = "id";
  public static final String TYPE_FIELD = "name";
  public static final String OBJECT_FIELD = "object";
  public static final String RELATIONS_FIELD = "relations";
  public static final String RELATIONS_ID = "dst_id";
  public static final String RELATIONS_NAME = "dst_name";

  private final ObjectMapper objectMapper;

  public PfbRecordConverter(ObjectMapper objectMapper) {
    super();
    this.objectMapper = objectMapper;
  }

  @Override
  Record createRecordShell(GenericRecord genRec) {
    return new Record(
        genRec.get(ID_FIELD).toString(),
        RecordType.valueOf(genRec.get(TYPE_FIELD).toString()),
        RecordAttributes.empty());
  }

  @Override
  protected final Record addAttributes(GenericRecord genRec, Record converted) {
    // extract the OBJECT_FIELD sub-record, then find all its attributes
    if (genRec.get(OBJECT_FIELD) instanceof GenericRecord objectAttributes) {
      return super.addAttributes(objectAttributes, converted, Set.of());
    }
    return converted;
  }

  @Override
  protected final Record addRelations(GenericRecord genRec, Record converted) {
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
      // According to its Javadoc, GenericData#toString() renders the given datum as JSON
      // However, it may contribute to numeric precision loss (see:
      // https://broadworkbench.atlassian.net/browse/AJ-1292)
      // If that's the case, then it may be necessary to traverse the record recursively and do a
      // less lossy conversion process.
      return GenericData.get().toString(recordAttr);
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
      return convertToString(mapAttr);
    }

    // Avro fixed
    if (attribute instanceof GenericFixed fixedAttr) {
      return new String(fixedAttr.bytes());
    }

    // Avro strings
    if (attribute instanceof CharSequence charSequenceAttr) {
      return charSequenceAttr.toString();
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

  private String convertToString(Object attribute) {
    try {
      return objectMapper.writeValueAsString(attribute);
    } catch (JsonProcessingException e) {
      LOGGER.warn(
          String.format(
              "Unable to convert attribute \"%s\" to JSON string, falling back to toString()",
              attribute),
          e);
      return attribute.toString();
    }
  }
}
