package org.databiosphere.workspacedataservice.dataimport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;

/** Logic to translate PFB/Avro schema definitions to WDS schema definitions. */
public class PfbSchemaConverter {

  /** Utility class to ease handling of array types */
  record PfbDataType(boolean isArray, Schema.Type schemaType) {}

  // ENUM datatype should be treated as a string
  // FIXED and BYTE datatypes will be treated as string, but unlikely to give decent output
  private static final Map<Schema.Type, DataTypeMapping> SCALAR_CONVERSIONS =
      Map.of(
          Schema.Type.BOOLEAN, DataTypeMapping.BOOLEAN,
          Schema.Type.DOUBLE, DataTypeMapping.NUMBER,
          Schema.Type.FLOAT, DataTypeMapping.NUMBER,
          Schema.Type.INT, DataTypeMapping.NUMBER,
          Schema.Type.LONG, DataTypeMapping.NUMBER,
          Schema.Type.MAP, DataTypeMapping.JSON,
          Schema.Type.RECORD, DataTypeMapping.JSON);

  // arrays of ENUM datatype should be treated as arrays of strings
  // arrays of FIXED or BYTE will be treated as arrays of string, but unlikely to give decent output
  // arrays of MAP or RECORD will be treated as arrays of string, but should be treated as array of
  //     json (AJ-1366)
  private static final Map<Schema.Type, DataTypeMapping> ARRAY_CONVERSIONS =
      Map.of(
          Schema.Type.BOOLEAN, DataTypeMapping.ARRAY_OF_BOOLEAN,
          Schema.Type.DOUBLE, DataTypeMapping.ARRAY_OF_NUMBER,
          Schema.Type.FLOAT, DataTypeMapping.ARRAY_OF_NUMBER,
          Schema.Type.INT, DataTypeMapping.ARRAY_OF_NUMBER,
          Schema.Type.LONG, DataTypeMapping.ARRAY_OF_NUMBER);

  /** return the WDS DataTypeMapping to use for a given PfbDataType */
  DataTypeMapping mapTypes(PfbDataType pfbDataType) {
    if (pfbDataType.isArray) {
      return ARRAY_CONVERSIONS.getOrDefault(
          pfbDataType.schemaType, DataTypeMapping.ARRAY_OF_STRING);
    } else {
      return SCALAR_CONVERSIONS.getOrDefault(pfbDataType.schemaType, DataTypeMapping.STRING);
    }
  }

  /** return the PfbDataType for a single field within the PFB's "object" column */
  PfbDataType getFieldSchema(Schema fieldSchema) {
    if (!fieldSchema.isUnion()) {
      boolean isArray = Schema.Type.ARRAY.equals(fieldSchema.getType());
      Schema.Type schemaType =
          isArray ? fieldSchema.getElementType().getType() : fieldSchema.getType();
      return new PfbDataType(isArray, schemaType);
    } else {
      // PFB types are often a union of NULL and some other type.
      // Ignore the nulls and just grab the type.
      Set<PfbDataType> nonNulls =
          fieldSchema.getTypes().stream()
              .filter(s -> s.getType() != Schema.Type.NULL)
              .map(this::getFieldSchema) // recurse
              .collect(Collectors.toSet());

      if (nonNulls.size() == 1) {
        return nonNulls.iterator().next();
      } else {
        throw new PfbParsingException(
            "found multiple schema types for field '" + fieldSchema + "': " + nonNulls);
      }
    }
  }

  /**
   * For a given record type within a PFB, return the WDS datatypes for all fields of that record
   * type
   */
  public Map<String, DataTypeMapping> pfbSchemaToWdsSchema(Schema pfbSchema) {
    Map<String, DataTypeMapping> result = new HashMap<>();

    List<Schema.Field> fields = pfbSchema.getFields();
    fields.forEach(
        fld -> {
          PfbDataType pfbDataType = getFieldSchema(fld.schema());
          result.put(fld.name(), mapTypes(pfbDataType));
        });

    return result;
  }
}
