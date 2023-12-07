package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter.RELATIONS_ID;
import static org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter.RELATIONS_NAME;
import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.OBJECT_SCHEMA;
import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.RELATION_ARRAY_SCHEMA;
import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.RELATION_SCHEMA;
import static org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES;
import static org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler.PfbImportMode.RELATIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PfbRecordConverterTest {

  // PFB "id" and "name" columns become the WDS Record id and type, respectively
  @Test
  void recordIdentifiers() {
    String inputId = RandomStringUtils.randomAlphanumeric(10);
    String inputName = RandomStringUtils.randomAlphanumeric(16);
    GenericRecord input = PfbTestUtils.makeRecord(inputId, inputName);

    Record actual = new PfbRecordConverter().genericRecordToRecord(input, BASE_ATTRIBUTES);

    assertEquals(inputId, actual.getId());
    assertEquals(inputName, actual.getRecordTypeName());
  }

  // if the GenericRecord has {} in the "object" column, the WDS record should have no attributes
  @Test
  void emptyObjectAttributes() {
    GenericRecord input = PfbTestUtils.makeRecord("my-id", "my-name");
    Record actual = new PfbRecordConverter().genericRecordToRecord(input, BASE_ATTRIBUTES);

    assertThat(actual.attributeSet()).isEmpty();
  }

  // if the GenericRecord has null in the "object" column, the WDS record should have no attributes
  @Test
  void nullObjectAttributes() {
    GenericRecord input =
        PfbTestUtils.makeRecord("this-record-has", "a-null-for-the-object-field", null);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input, BASE_ATTRIBUTES);

    assertThat(actual.attributeSet()).isEmpty();
  }

  // "smoke test" unit test: given an Avro GenericRecord containing a variety of values,
  // convert to a WDS record and assert correctness of that WDS record.
  @Test
  void valuesInObjectAttributes() {
    Schema enumSchema =
        Schema.createEnum("name", "doc", "namespace", List.of("enumValue1", "enumValue2"));

    Schema fixedTenBytes =
        Schema.createFixed("fixedBytes", /* doc= */ null, /* space= */ null, /* size= */ 10);
    Schema myObjSchema =
        Schema.createRecord(
            "mytype",
            "doc",
            "namespace",
            false,
            List.of(
                new Schema.Field("marco", Schema.create(Schema.Type.STRING)),
                new Schema.Field("pi", Schema.create(Schema.Type.LONG)),
                new Schema.Field("afile", Schema.create(Schema.Type.STRING)),
                new Schema.Field("booly", Schema.create(Schema.Type.BOOLEAN)),
                new Schema.Field("enum", enumSchema),
                new Schema.Field("bytesOfStuff", Schema.create(Schema.Type.BYTES)),
                new Schema.Field("tenFixedBytesOfStuff", fixedTenBytes),
                new Schema.Field(
                    "arrayOfNumbers", Schema.createArray(Schema.create(Schema.Type.LONG))),
                new Schema.Field(
                    "arrayOfStrings", Schema.createArray(Schema.create(Schema.Type.STRING))),
                new Schema.Field("arrayOfEnums", Schema.createArray(enumSchema))));

    GenericData.Record objectAttributes = new GenericData.Record(myObjSchema);
    objectAttributes.put("marco", "polo");
    objectAttributes.put("pi", 3.14159);
    objectAttributes.put("afile", "https://some/path/to/a/file");
    objectAttributes.put("booly", Boolean.TRUE);
    objectAttributes.put(
        "enum", new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue2"));
    objectAttributes.put("bytesOfStuff", ByteBuffer.wrap("some bytes".getBytes()));
    objectAttributes.put(
        "tenFixedBytesOfStuff", new GenericData.Fixed(fixedTenBytes, "fixedBytes".getBytes()));
    objectAttributes.put("arrayOfNumbers", List.of(1.2, 3.4));
    objectAttributes.put("arrayOfStrings", List.of("one", "two", "three"));
    objectAttributes.put(
        "arrayOfEnums",
        List.of(
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue2"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue1")));

    GenericRecord input = PfbTestUtils.makeRecord("my-id", "mytype", objectAttributes);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input, BASE_ATTRIBUTES);

    Set<Map.Entry<String, Object>> actualAttributeSet = actual.attributeSet();
    Set<String> actualKeySet =
        actualAttributeSet.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertEquals(
        Set.of(
            "marco",
            "pi",
            "afile",
            "booly",
            "enum",
            "bytesOfStuff",
            "tenFixedBytesOfStuff",
            "arrayOfNumbers",
            "arrayOfStrings",
            "arrayOfEnums"),
        actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    assertEquals(BigDecimal.valueOf(3.14159), actual.getAttributeValue("pi"));
    assertEquals(Boolean.TRUE, actual.getAttributeValue("booly"));
    assertEquals("enumValue2", actual.getAttributeValue("enum"));
    assertEquals("some bytes", actual.getAttributeValue("bytesOfStuff"));
    assertEquals("fixedBytes", actual.getAttributeValue("tenFixedBytesOfStuff"));
    assertEquals(
        List.of(BigDecimal.valueOf(1.2), BigDecimal.valueOf(3.4)),
        actual.getAttributeValue("arrayOfNumbers"));
    assertEquals(List.of("one", "two", "three"), actual.getAttributeValue("arrayOfStrings"));
    assertEquals(List.of("enumValue2", "enumValue1"), actual.getAttributeValue("arrayOfEnums"));
  }

  @Test
  void relationsInRecord() {
    GenericData.Record relation = new GenericData.Record(RELATION_SCHEMA);
    relation.put(RELATIONS_ID, "relation_id");
    relation.put(RELATIONS_NAME, "relation_table");
    GenericData.Array relations = new GenericData.Array(RELATION_ARRAY_SCHEMA, List.of(relation));

    GenericRecord input =
        PfbTestUtils.makeRecord(
            "my-id", "mytype", new GenericData.Record(OBJECT_SCHEMA), relations);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input, RELATIONS);

    assertEquals(
        RelationUtils.createRelationString(RecordType.valueOf("relation_table"), "relation_id"),
        actual.getAttributeValue("relation_table"));
  }

  // arguments for parameterized test, in the form of: input value, expected return value
  static Stream<Arguments> provideConvertScalarAttributesArgs() {
    return Stream.of(
        // most basic case
        Arguments.of("hello", "hello"),
        // null inputs
        Arguments.of(null, null),
        // numbers
        Arguments.of(Long.MIN_VALUE, BigDecimal.valueOf(Long.MIN_VALUE)),
        Arguments.of(Long.MAX_VALUE, BigDecimal.valueOf(Long.MAX_VALUE)),
        Arguments.of(Integer.MIN_VALUE, BigDecimal.valueOf(Integer.MIN_VALUE)),
        Arguments.of(Integer.MAX_VALUE, BigDecimal.valueOf(Integer.MAX_VALUE)),
        Arguments.of(Float.MIN_VALUE, BigDecimal.valueOf(Float.MIN_VALUE)),
        Arguments.of(Float.MAX_VALUE, BigDecimal.valueOf(Float.MAX_VALUE)),
        Arguments.of(Double.MIN_VALUE, BigDecimal.valueOf(Double.MIN_VALUE)),
        Arguments.of(Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE)),
        // booleans
        Arguments.of(true, true),
        Arguments.of(false, false));
  }

  // targeted test for converting scalar Avro values to WDS values
  @ParameterizedTest(name = "with input of {0}, return value should be {2}")
  @MethodSource("provideConvertScalarAttributesArgs")
  void convertScalarAttributes(Object input, Object expected) {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Object actual = pfbRecordConverter.convertAttributeType(input);
    assertEquals(expected, actual);
  }

  // targeted test for converting scalar Avro enums to WDS values
  @Test
  void convertScalarEnums() {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Object input = new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bar");

    Object actual = pfbRecordConverter.convertAttributeType(input);
    assertEquals("bar", actual);
  }

  // arguments for parameterized test, in the form of: input value, expected return value
  static Stream<Arguments> provideConvertArrayAttributesArgs() {
    return Stream.of(
        // most basic case
        Arguments.of(List.of("hello", "world"), List.of("hello", "world")),
        // null inputs
        Arguments.of(null, null),
        // empty arrays
        Arguments.of(List.of(), List.of()),
        // numbers
        Arguments.of(
            List.of(Long.MIN_VALUE, 1L, Long.MAX_VALUE),
            List.of(
                BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(1L),
                BigDecimal.valueOf(Long.MAX_VALUE))),
        Arguments.of(
            List.of(Integer.MIN_VALUE, 1, Integer.MAX_VALUE),
            List.of(
                BigDecimal.valueOf(Integer.MIN_VALUE),
                BigDecimal.valueOf(1),
                BigDecimal.valueOf(Integer.MAX_VALUE))),
        Arguments.of(
            List.of(Float.MIN_VALUE, 1F, Float.MAX_VALUE),
            List.of(
                BigDecimal.valueOf(Float.MIN_VALUE),
                BigDecimal.valueOf(1F),
                BigDecimal.valueOf(Float.MAX_VALUE))),
        Arguments.of(
            List.of(Double.MIN_VALUE, 1D, Double.MAX_VALUE),
            List.of(
                BigDecimal.valueOf(Double.MIN_VALUE),
                BigDecimal.valueOf(1D),
                BigDecimal.valueOf(Double.MAX_VALUE))),
        // booleans
        Arguments.of(List.of(true, false, true), List.of(true, false, true)));
  }

  // targeted test for converting array Avro values to WDS values
  @ParameterizedTest(name = "with array input of {0}, return value should be {2}")
  @MethodSource("provideConvertArrayAttributesArgs")
  void convertArrayAttributes(Object input, Object expected) {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Object actual = pfbRecordConverter.convertAttributeType(input);
    assertEquals(expected, actual);
  }

  // targeted test for converting array Avro enums to WDS values
  @Test
  void convertArrayOfEnums() {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Object input =
        List.of(
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bar"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "foo"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "baz"));

    Object actual = pfbRecordConverter.convertAttributeType(input);
    assertEquals(List.of("bar", "foo", "baz"), actual);
  }

  @Test
  void decodesEnumsCorrectly() {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Object input =
        List.of(
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bpm_20__3E__20_60"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bpm_20__3E__20_80"),
            new GenericData.EnumSymbol(
                Schema.create(Schema.Type.STRING), "only_20_space_20_conversions"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "noconversion"));

    Object actual = pfbRecordConverter.convertAttributeType(input);
    assertEquals(List.of("bpm > 60", "bpm > 80", "only space conversions", "noconversion"), actual);
  }
}
