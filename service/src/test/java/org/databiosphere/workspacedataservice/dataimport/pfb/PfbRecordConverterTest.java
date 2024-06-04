package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter.RELATIONS_ID;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter.RELATIONS_NAME;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.OBJECT_SCHEMA;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.RELATION_ARRAY_SCHEMA;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.RELATION_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.service.JsonConfig;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest(classes = {JsonConfig.class, PfbRecordConverter.class})
class PfbRecordConverterTest extends TestBase {

  @Autowired private PfbRecordConverter converter;

  // PFB "id" and "name" columns become the WDS Record id and type, respectively
  @Test
  void recordIdentifiers() {
    String inputId = RandomStringUtils.randomAlphanumeric(10);
    String inputName = RandomStringUtils.randomAlphanumeric(16);
    GenericRecord input = PfbTestUtils.makeRecord(inputId, inputName);

    Record actual = converter.convert(input, ImportMode.BASE_ATTRIBUTES);

    assertEquals(inputId, actual.getId());
    assertEquals(inputName, actual.getRecordTypeName());
  }

  // if the GenericRecord has {} in the "object" column, the WDS record should have no attributes
  @Test
  void emptyObjectAttributes() {
    GenericRecord input = PfbTestUtils.makeRecord("my-id", "my-name");
    Record actual = converter.convert(input, ImportMode.BASE_ATTRIBUTES);

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
    Schema embeddedObjectSchema =
        Schema.createRecord(
            "embeddedType",
            "doc",
            "namespace",
            false,
            List.of(
                new Schema.Field("embeddedLong", Schema.create(Schema.Type.LONG)),
                new Schema.Field("embeddedString", Schema.create(Schema.Type.STRING))));

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
                new Schema.Field("arrayOfEnums", Schema.createArray(enumSchema)),
                new Schema.Field("mapOfNumbers", Schema.createMap(Schema.create(Schema.Type.LONG))),
                new Schema.Field(
                    "mapOfStrings", Schema.createMap(Schema.create(Schema.Type.STRING))),
                new Schema.Field("mapOfEnums", Schema.createMap(enumSchema)),
                new Schema.Field("embeddedObject", embeddedObjectSchema)));

    GenericData.Record embeddedObjectRecord =
        new GenericRecordBuilder(embeddedObjectSchema)
            .set("embeddedLong", 123L)
            .set("embeddedString", "embeddedString")
            .build();
    GenericData.Record objectAttributes =
        new GenericRecordBuilder(myObjSchema)
            .set("marco", "polo")
            .set("pi", 3.14159)
            .set("afile", "https://some/path/to/a/file")
            .set("booly", Boolean.TRUE)
            .set(
                "enum", new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue2"))
            .set("bytesOfStuff", ByteBuffer.wrap("some bytes".getBytes()))
            .set(
                "tenFixedBytesOfStuff",
                new GenericData.Fixed(fixedTenBytes, "fixedBytes".getBytes()))
            .set("arrayOfNumbers", List.of(1.2, 3.4))
            .set("arrayOfStrings", List.of("one", "two", "three"))
            .set(
                "arrayOfEnums",
                List.of(
                    new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue2"),
                    new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue1")))
            .set("mapOfNumbers", Map.of("one", 1L, "two", 2L, "three", 3L))
            .set("mapOfStrings", Map.of("one", "one", "two", "two", "three", "three"))
            .set("mapOfEnums", Map.of("one", "enumValue1", "two", "enumValue2"))
            .set("embeddedObject", embeddedObjectRecord)
            .build();
    GenericRecord input = PfbTestUtils.makeRecord("my-id", "mytype", objectAttributes);
    Record actual = converter.convert(input, ImportMode.BASE_ATTRIBUTES);

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
            "arrayOfEnums",
            "mapOfNumbers",
            "mapOfStrings",
            "mapOfEnums",
            "embeddedObject"),
        actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    assertEquals(BigDecimal.valueOf(3.14159), actual.getAttributeValue("pi"));
    assertEquals(Boolean.TRUE, actual.getAttributeValue("booly"));
    assertEquals("enumValue2", actual.getAttributeValue("enum"));
    assertEquals(
        "[115, 111, 109, 101, 32, 98, 121, 116, 101, 115]",
        actual.getAttributeValue("bytesOfStuff"),
        "\"some bytes\" displayed as the array of bytes");
    assertEquals(
        "[102, 105, 120, 101, 100, 66, 121, 116, 101, 115]",
        actual.getAttributeValue("tenFixedBytesOfStuff"),
        "\"fixedBytes\" displayed as the array of bytes");
    assertEquals(
        List.of(BigDecimal.valueOf(1.2), BigDecimal.valueOf(3.4)),
        actual.getAttributeValue("arrayOfNumbers"));
    assertEquals(List.of("one", "two", "three"), actual.getAttributeValue("arrayOfStrings"));
    assertEquals(List.of("enumValue2", "enumValue1"), actual.getAttributeValue("arrayOfEnums"));

    // json attributes use the JsonAttribute wrapper class
    assertEqualsJsonAttribute(
        "{\"one\":1,\"two\":2,\"three\":3}", actual.getAttributeValue("mapOfNumbers"));
    assertEqualsJsonAttribute(
        "{\"one\":\"one\",\"two\":\"two\",\"three\":\"three\"}",
        actual.getAttributeValue("mapOfStrings"));
    assertEqualsJsonAttribute(
        "{\"one\":\"enumValue1\",\"two\":\"enumValue2\"}", actual.getAttributeValue("mapOfEnums"));
    assertEqualsJsonAttribute(
        "{\"embeddedLong\":123,\"embeddedString\":\"embeddedString\"})",
        actual.getAttributeValue("embeddedObject"));
  }

  @Test
  void relationsInRecord() {
    GenericData.Record relation = new GenericData.Record(RELATION_SCHEMA);
    relation.put(RELATIONS_ID, "relation_id");
    relation.put(RELATIONS_NAME, "relation_table");
    GenericData.Array<GenericData.Record> relations =
        new GenericData.Array<>(RELATION_ARRAY_SCHEMA, List.of(relation));

    GenericRecord input =
        PfbTestUtils.makeRecord(
            "my-id", "mytype", new GenericData.Record(OBJECT_SCHEMA), relations);
    Record actual = converter.convert(input, ImportMode.RELATIONS);

    assertEquals(
        new RelationAttribute(RecordType.valueOf("relation_table"), "relation_id"),
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
  @ParameterizedTest(name = "with input of {0}, return value should be {1}")
  @MethodSource("provideConvertScalarAttributesArgs")
  void convertScalarAttributes(Object input, Object expected) {
    Object actual = converter.convertAttributeType(input);
    assertEquals(expected, actual);
  }

  // targeted test for converting scalar Avro enums to WDS values
  @Test
  void convertScalarEnums() {
    Object input = new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bar");

    Object actual = converter.convertAttributeType(input);
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
  @ParameterizedTest(name = "with array input of {0}, return value should be {1}")
  @MethodSource("provideConvertArrayAttributesArgs")
  void convertArrayAttributes(Object input, Object expected) {
    Object actual = converter.convertAttributeType(input);
    assertEquals(expected, actual);
  }

  // targeted test for converting array Avro enums to WDS values
  @Test
  void convertArrayOfEnums() {
    Object input =
        List.of(
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bar"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "foo"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "baz"));

    Object actual = converter.convertAttributeType(input);
    assertEquals(List.of("bar", "foo", "baz"), actual);
  }

  @ParameterizedTest(
      name = "with an enum value of {0} embedded in a list, return value should include {1}")
  @MethodSource("provideDecodeEnumArgs")
  void decodesEnums(String symbol, String expected) {
    Object input = List.of(new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), symbol));
    Object actual = converter.convertAttributeType(input);
    assertEquals(List.of(expected), actual);
  }

  // arguments for parameterized test, in the form of: input value, expected return value
  static Stream<Arguments> provideDecodeEnumArgs() {
    return Stream.of(
        Arguments.of("bpm_20__3E__20_60", "bpm > 60"),
        Arguments.of("bpm_20__3E__20_80", "bpm > 80"),
        Arguments.of("only_20_space_20_conversions", "only space conversions"),
        Arguments.of("noconversion", "noconversion"));
  }

  private void assertEqualsJsonAttribute(String expectedJsonString, Object actualValue) {
    // json attributes use the JsonAttribute wrapper class
    JsonAttribute actual = assertInstanceOf(JsonAttribute.class, actualValue);
    try {
      JsonNode actualJson = normalizeJson(actual.jsonValue().toString());
      JsonNode expectedJson = normalizeJson(expectedJsonString);
      assertThat(actualJson).isEqualTo(expectedJson);
    } catch (JsonProcessingException e) {
      fail(
          String.format(
              "Unable to parse JSON strings for comparison. Expected: "
                  + expectedJsonString
                  + " Actual: "
                  + actualValue),
          e);
    }
  }

  private JsonNode normalizeJson(String jsonString) throws JsonProcessingException {
    ObjectMapper objectMapper = new JsonConfig().objectMapper();
    JsonNode tree = objectMapper.readTree(jsonString);

    // Convert JSON tree to a canonical string representation
    String normalizedJson = objectMapper.writeValueAsString(tree);

    // Parse the normalized string back to a JsonNode
    return objectMapper.readTree(normalizedJson);
  }
}
