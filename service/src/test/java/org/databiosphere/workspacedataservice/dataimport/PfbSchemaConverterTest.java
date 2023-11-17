package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class PfbSchemaConverterTest {

  // ========== start tests for scalar datatypes

  @Test
  void mapScalarBoolean() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.BOOLEAN,
        pfbSchemaConverter.mapTypes(
            new PfbSchemaConverter.PfbDataType(false, Schema.Type.BOOLEAN)));
  }

  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"DOUBLE", "FLOAT", "INT", "LONG"})
  void mapScalarNumbers(Schema.Type pfbType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.NUMBER,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(false, pfbType)));
  }

  // WDS treats enums as strings; this may change at some point
  @Test
  void mapScalarEnum() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(false, Schema.Type.ENUM)));
  }

  @Test
  void mapScalarString() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(false, Schema.Type.STRING)));
  }

  // WDS treats fixed and bytes as strings; this may change at some point
  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"FIXED", "BYTES"})
  void mapScalarBinaries(Schema.Type pfbType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(false, pfbType)));
  }

  @Disabled("JSON types not handled yet in PfbRecordConverter")
  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"RECORD", "MAP"})
  void mapScalarJsons(Schema.Type pfbType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.JSON,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(false, pfbType)));
  }

  // ========== start tests for array datatypes

  @Test
  void mapArrayBoolean() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.ARRAY_OF_BOOLEAN,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(true, Schema.Type.BOOLEAN)));
  }

  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"DOUBLE", "FLOAT", "INT", "LONG"})
  void mapArrayNumbers(Schema.Type pfbType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.ARRAY_OF_NUMBER,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(true, pfbType)));
  }

  // WDS treats enums as strings; this may change at some point
  @Test
  void mapArrayEnum() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.ARRAY_OF_STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(true, Schema.Type.ENUM)));
  }

  @Test
  void mapArrayString() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.ARRAY_OF_STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(true, Schema.Type.STRING)));
  }

  // WDS treats fixed and bytes as strings; this may change at some point
  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"FIXED", "BYTES"})
  void mapArrayBinaries(Schema.Type pfbType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.ARRAY_OF_STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(true, pfbType)));
  }

  // AJ-1366: array of json not supported; these convert to array of string.
  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"RECORD", "MAP"})
  void mapArrayJsons(Schema.Type pfbType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    assertEquals(
        DataTypeMapping.ARRAY_OF_STRING,
        pfbSchemaConverter.mapTypes(new PfbSchemaConverter.PfbDataType(true, pfbType)));
  }

  // ========== start tests for getFieldSchema

  @ParameterizedTest(name = "for schema with array = {0}")
  @ValueSource(booleans = {true, false})
  void arrayConversion(Boolean isArray) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Schema schema;
    if (isArray) {
      schema = Schema.createArray(Schema.create(Schema.Type.LONG));
    } else {
      schema = Schema.create(Schema.Type.LONG);
    }
    PfbSchemaConverter.PfbDataType actual = pfbSchemaConverter.getFieldSchema(schema);
    assertEquals(isArray, actual.isArray());
  }

  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"DOUBLE", "STRING", "BOOLEAN"}) // spot-check a few types
  void unionWithNull(Schema.Type avroType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Schema schema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(avroType));
    PfbSchemaConverter.PfbDataType actual = pfbSchemaConverter.getFieldSchema(schema);
    assertEquals(avroType, actual.schemaType());
  }

  @Test
  void unionEnumWithNull() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Schema enumSchema =
        Schema.createEnum("name", "doc", "namespace", List.of("enumValue1", "enumValue2"));
    Schema schema = Schema.createUnion(Schema.create(Schema.Type.NULL), enumSchema);
    PfbSchemaConverter.PfbDataType actual = pfbSchemaConverter.getFieldSchema(schema);
    assertEquals(Schema.Type.ENUM, actual.schemaType());
  }

  @ParameterizedTest(name = "when converting {0}")
  @EnumSource(
      value = Schema.Type.class,
      names = {"DOUBLE", "STRING", "BOOLEAN"}) // spot-check a few types
  void unionWithAnotherType(Schema.Type avroType) {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Schema schema = Schema.createUnion(Schema.create(Schema.Type.LONG), Schema.create(avroType));
    assertThrows(PfbParsingException.class, () -> pfbSchemaConverter.getFieldSchema(schema));
  }

  @Test
  void unionEnumWithAnotherType() {
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Schema enumSchema =
        Schema.createEnum("name", "doc", "namespace", List.of("enumValue1", "enumValue2"));
    Schema schema = Schema.createUnion(Schema.create(Schema.Type.LONG), enumSchema);
    assertThrows(PfbParsingException.class, () -> pfbSchemaConverter.getFieldSchema(schema));
  }

  // ========== start tests for pfbSchemaToWdsSchema

  @Test
  void pfbSchemaToWdsSchema() {
    List<Schema.Field> fields =
        List.of(
            new Schema.Field("string", Schema.create(Schema.Type.STRING)),
            new Schema.Field("string_array", Schema.createArray(Schema.create(Schema.Type.STRING))),
            new Schema.Field("number", Schema.create(Schema.Type.LONG)),
            new Schema.Field("number_array", Schema.createArray(Schema.create(Schema.Type.INT))),
            new Schema.Field(
                "union_bool",
                Schema.createUnion(
                    Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN))));

    Schema schema = Schema.createRecord("name", "doc", "namespace", false, fields);

    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();

    Map<String, DataTypeMapping> expected =
        Map.of(
            "string", DataTypeMapping.STRING,
            "string_array", DataTypeMapping.ARRAY_OF_STRING,
            "number", DataTypeMapping.NUMBER,
            "number_array", DataTypeMapping.ARRAY_OF_NUMBER,
            "union_bool", DataTypeMapping.BOOLEAN);

    Map<String, DataTypeMapping> actual = pfbSchemaConverter.pfbSchemaToWdsSchema(schema);

    assertEquals(expected, actual);
  }
}
