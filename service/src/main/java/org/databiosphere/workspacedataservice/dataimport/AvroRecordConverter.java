package org.databiosphere.workspacedataservice.dataimport;

import static bio.terra.pfb.PfbReader.convertEnum;
import static org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler.PfbImportMode.RELATIONS;

import com.google.mu.util.stream.BiStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logic to convert Avro GenericRecord to WDS's Record Avro GenericRecord is used by PFB import and
 * TDR import.
 */
public abstract class AvroRecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordConverter.class);

  public AvroRecordConverter() {}

  /**
   * Create the "shell" Record, with a valid RecordId and a valid RecordType, but no attributes.
   * Subclasses must implement this method, which is called for every GenericRecord in the inbound
   * import data.
   *
   * @param genRec the Avro GenericRecord to be converted
   * @return the WDS Record shell
   */
  abstract Record createRecordShell(GenericRecord genRec);

  abstract Record addAttributes(GenericRecord objectAttributes, Record converted);

  abstract Record addRelations(GenericRecord genRec, Record converted);

  public Record genericRecordToRecord(
      GenericRecord genRec, PfbStreamWriteHandler.PfbImportMode pfbImportMode) {
    // create the WDS record shell (id, record type, empty attributes)
    Record converted = createRecordShell(genRec);
    if (pfbImportMode == RELATIONS) {
      return addRelations(genRec, converted);
    } else {
      return addAttributes(genRec, converted);
    }
  }

  protected Record addAttributes(
      GenericRecord objectAttributes, Record converted, Set<String> ignoreAttributes) {
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
    converted.setAttributes(attributes);
    return converted;
  }

  protected Object convertAttributeType(Object attribute) {

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

    // We don't see this type in PFB files, is this specific to parquet-mr?
    if (attribute instanceof org.apache.avro.util.Utf8 utf8data) {
      return new String(utf8data.getBytes(), StandardCharsets.UTF_8);
    }

    LOGGER.warn(
        "convertAttributeType received value \"{}\" with unexpected type {}",
        attribute,
        attribute.getClass());
    return attribute.toString();
  }
}
