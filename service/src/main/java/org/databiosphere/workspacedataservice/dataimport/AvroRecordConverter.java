package org.databiosphere.workspacedataservice.dataimport;

import static bio.terra.pfb.PfbReader.convertEnum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

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
              : convertAttributeType(
                  destructureElementList(objectAttributes.get(fieldName), field), field);

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
  @Nullable
  public Object convertAttributeType(@Nullable Object attribute, Field field) {

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
      String serialized = GenericData.get().toString(recordAttr);
      return createJsonAttribute(serialized);
    }

    // Avro enums
    if (attribute instanceof GenericEnumSymbol<?> enumAttr) {
      return convertEnum(enumAttr.toString());
    }

    // Avro arrays
    if (attribute instanceof Collection<?> collAttr) {
      // recurse
      return collAttr.stream().map(value -> this.convertAttributeType(value, field)).toList();
    }

    // Avro maps
    if (attribute instanceof Map<?, ?> mapAttr) {
      String serialized = convertToString(mapAttr);
      return createJsonAttribute(serialized);
    }

    // Avro fixed
    if (attribute instanceof GenericData.Fixed fixedAttr) {
      return convertFixed(fixedAttr, getLogicalTypeForValues(field));
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
      return convertInteger(intAttr, getLogicalTypeForValues(field));
    }

    // Avro longs
    if (attribute instanceof Long longAttr) {
      return convertLong(longAttr, getLogicalTypeForValues(field));
    }

    // Avro floats
    if (attribute instanceof Float floatAttr) {
      if (Float.isNaN(floatAttr)) {
        return null;
      }
      return BigDecimal.valueOf(floatAttr);
    }

    // Avro doubles
    if (attribute instanceof Double doubleAttr) {
      if (Double.isNaN(doubleAttr)) {
        return null;
      }
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

  /**
   * Checks if this Field is a structured element list; if it is, this method destructures the list
   * and returns its elements.
   *
   * @see #isStructuredList(Field)
   * @param attrValue the value to potentially destructure
   * @param field the schema field to inspect for structure
   * @return destructured elements, or the original input attrValue if not a structured list
   */
  @Nullable
  protected Object destructureElementList(@Nullable Object attrValue, Field field) {
    // if this is a structured parquet list, destructure it
    if (attrValue instanceof Collection<?> collAttr && isStructuredList(field)) {
      return collAttr.stream()
          .map(
              element -> {
                GenericRecord elementRecord =
                    (GenericRecord) element; // cast is safe due to isStructuredList condition
                return elementRecord.get(0); // get is safe due to isStructuredList condition
              })
          .toList();
    } else {
      return attrValue;
    }
  }

  /**
   * Is this Field a structured element list? We see structured element lists from Parquet; see <a
   * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">doc</a>.
   * Parquet uses these structures to enforce constraints around nullability and repeatability. User
   * data in the form of [1, 2] is serialized as [{"element":1},{"element":2}]. This method detects
   * such a structure.
   *
   * @param field the schema field to inspect
   * @return whether this is a Parquet structured list
   */
  private boolean isStructuredList(Field field) {
    // field must be declared as an array
    if (Schema.Type.ARRAY.equals(field.schema().getType())) {
      Schema elementSchema = field.schema().getElementType();
      return Schema.Type.RECORD.equals(elementSchema.getType()) // elements must be records
          && elementSchema.getFields().size() == 1 // with only a single field
          // and the sub-element is named "list", "array", or "${field.name()}_tuple"
          && (elementSchema.getName().equals("list")
              || elementSchema.getName().equals("array")
              || elementSchema.getName().equals(field.name() + "_tuple"));
    }
    return false;
  }

  /**
   * Get the logical type of values in a field. If the field is a list, this returns the logical
   * type of the list elements.
   */
  private @Nullable LogicalType getLogicalTypeForValues(Field field) {
    // Lists
    if (isStructuredList(field)) {
      return field.schema().getElementType().getFields().get(0).schema().getLogicalType();
    }
    if (Schema.Type.ARRAY.equals(field.schema().getType())) {
      return field.schema().getElementType().getLogicalType();
    }
    // For scalar fields, Parquet files exported by TDR have a logical type
    // that is a union of "null" and the actual data type.
    if (Schema.Type.UNION.equals(field.schema().getType())) {
      List<Schema> nonNullTypes =
          field.schema().getTypes().stream()
              .filter(type -> !Schema.Type.NULL.equals(type.getType()))
              .toList();
      if (nonNullTypes.size() > 1) {
        LOGGER.warn(
            "getLogicalTypeForValue received field with multiple types: {}", field.schema());
        return null;
      }
      return nonNullTypes.get(0).getLogicalType();
    }
    return field.schema().getLogicalType();
  }

  // Parquet has a concept of "physical type" and "logical type". For some numeric types,
  // the physical type is a fixed-size byte array (GenericData.Fixed), but the logical type
  // defines the desired shape of the value, such as Decimal. This method converts the fixed-size
  // byte arrays to decimals.
  private Object convertFixed(GenericData.Fixed fixedAttr, @Nullable LogicalType logicalType) {
    // handle logical decimals
    if (logicalType instanceof LogicalTypes.Decimal) {
      // transform the bytes into a BigDecimal. This BigDecimal will have scale and precision
      // specified by the schema, which can be much larger than the actual values and leads
      // to trailing zeros in the decimal part of the number
      BigDecimal bigDecimal =
          new Conversions.DecimalConversion()
              .fromFixed(fixedAttr, fixedAttr.getSchema(), logicalType);

      // now, try to turn the BigDecimal back into an int or a double; this truncates the
      // precision to the actual value. Example: 0.220000000 will become 0.22
      try {
        return bigDecimal.intValueExact();
      } catch (ArithmeticException ae) {
        return bigDecimal.doubleValue();
      }
    }

    // here, we could handle other logical types such as date or the various timestamps

    // if this was NOT a logical decimal, just toString() it so we have some usable value
    return fixedAttr.toString();
  }

  private Object convertInteger(Integer intAttr, @Nullable LogicalType logicalType) {
    if (logicalType instanceof LogicalTypes.Date) {
      return LocalDate.ofEpochDay(intAttr);
    }

    return BigDecimal.valueOf(intAttr);
  }

  private Object convertLong(Long longAttr, @Nullable LogicalType logicalType) {
    if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
      long seconds = Math.floorDiv(longAttr, 1000000);
      int nanoSeconds = Math.floorMod(longAttr, 1000000);
      return LocalDateTime.ofEpochSecond(seconds, nanoSeconds, ZoneOffset.UTC);
    }

    if (logicalType instanceof LogicalTypes.TimeMicros) {
      LocalTime time = LocalTime.ofNanoOfDay(longAttr * 1000);
      return time.format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    return BigDecimal.valueOf(longAttr);
  }

  private JsonAttribute createJsonAttribute(String serialized) {
    try {
      return new JsonAttribute(objectMapper.readTree(serialized));
    } catch (JsonProcessingException e) {
      throw new DataImportException("Unable to convert to JsonAttribute: " + e.getMessage(), e);
    }
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
