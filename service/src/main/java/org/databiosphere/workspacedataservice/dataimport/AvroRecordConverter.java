package org.databiosphere.workspacedataservice.dataimport;

import static bio.terra.pfb.PfbReader.convertEnum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.recordsource.TwoPassRecordSource.ImportMode;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to convert Avro GenericRecord to WDS's Record. Used by PFB import and TDR import. */
public abstract class AvroRecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordConverter.class);

  private final ObjectMapper objectMapper;

  public AvroRecordConverter(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * Converts an Avro record to a WDS record. In {@link ImportMode#BASE_ATTRIBUTES} mode, this will
   * call {@link #convertBaseAttributes(GenericRecord)} with the intention of creating a Record
   * containing all non-relation attributes. In {@link ImportMode#RELATIONS} mode, this will call
   * {@link #convertRelations(GenericRecord)} with the intention of creating a Record containing
   * only relation attributes.
   */
  public Record convert(GenericRecord genRec, ImportMode importMode) {
    return switch (importMode) {
      case RELATIONS -> convertRelations(genRec);
      case BASE_ATTRIBUTES -> convertBaseAttributes(genRec);
    };
  }

  /**
   * When operating in {@see ImportMode.BASE_ATTRIBUTES} mode, what Record - and its attributes -
   * should be upserted?
   *
   * @param genericRecord the inbound Avro GenericRecord to be converted
   * @return the Record containing base (non-relation) WDS attributes
   */
  protected abstract Record convertBaseAttributes(GenericRecord genericRecord);

  /**
   * When operating in {@see ImportMode.RELATIONS} mode, what Record - and its attributes - should
   * be upserted?
   *
   * @param genericRecord the inbound Avro GenericRecord to be converted
   * @return the Record containing WDS relation attributes
   */
  protected abstract Record convertRelations(GenericRecord genericRecord);

  /**
   * Extract WDS attributes from an Avro GenericRecord, optionally skipping over a set of
   * field/attribute names. Subclasses can call this, as it implements logic shared between PFB and
   * Parquet import.
   *
   * @param objectAttributes the Avro record from which to extract WDS attributes
   * @param ignoreAttributes field names that should not be extracted from the Avro record
   * @return the extracted attributes
   */
  protected RecordAttributes extractBaseAttributes(
      GenericRecord objectAttributes, Set<String> ignoreAttributes) {
    // loop over all Avro fields and add to the record's attributes
    Schema schema = objectAttributes.getSchema();
    List<Schema.Field> fields = schema.getFields();
    RecordAttributes attributes = RecordAttributes.empty();
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      // if this attribute is marked as ignorable, skip it
      if (ignoreAttributes.contains(fieldName)) {
        continue;
      }
      Object value =
          objectAttributes.get(fieldName) == null
              ? null
              : convertAttributeType(objectAttributes.get(fieldName));
      attributes.putAttribute(fieldName, value);
    }
    return attributes;
  }

  /**
   * Converts a single Avro field's to a WDS attribute value.
   *
   * @param attribute the Avro field
   * @return the WDS attribute value
   */
  @VisibleForTesting
  public Object convertAttributeType(Object attribute) {

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
    if (attribute instanceof GenericData.Fixed fixedAttr) {
      return fixedAttr.toString();
    }

    // Avro strings
    if (attribute instanceof CharSequence charSequenceAttr) {
      return charSequenceAttr.toString();
    }

    // Avro bytes
    if (attribute instanceof ByteBuffer byteBufferAttr) {
      // copy the behavior of GenericData.Fixed.toString()
      // to protect against null bytes that may be present in the buffer
      return Arrays.toString(byteBufferAttr.array());
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
