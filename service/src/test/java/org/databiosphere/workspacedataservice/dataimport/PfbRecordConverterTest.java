package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;
import org.databiosphere.workspacedataservice.shared.model.Record;
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

    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    assertEquals(inputId, actual.getId());
    assertEquals(inputName, actual.getRecordTypeName());
  }

  // if the GenericRecord has {} in the "object" column, the WDS record should have no attributes
  @Test
  void emptyObjectAttributes() {
    GenericRecord input = PfbTestUtils.makeRecord("my-id", "my-name");
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    assertThat(actual.attributeSet()).isEmpty();
  }

  // if the GenericRecord has null in the "object" column, the WDS record should have no attributes
  @Test
  void nullObjectAttributes() {
    GenericRecord input =
        PfbTestUtils.makeRecord("this-record-has", "a-null-for-the-object-field", null);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    assertThat(actual.attributeSet()).isEmpty();
  }

  // "smoke test" unit test: given an Avro GenericRecord containing a variety of values,
  // convert to a WDS record and assert correctness of that WDS record.
  @Test
  void valuesInObjectAttributes() {
    Schema enumSchema =
        Schema.createEnum("name", "doc", "namespace", List.of("enumValue1", "enumValue2"));

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
    objectAttributes.put("arrayOfNumbers", List.of(1.2, 3.4));
    objectAttributes.put("arrayOfStrings", List.of("one", "two", "three"));
    objectAttributes.put(
        "arrayOfEnums",
        List.of(
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue2"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "enumValue1")));

    GenericRecord input = PfbTestUtils.makeRecord("my-id", "mytype", objectAttributes);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

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
            "arrayOfNumbers",
            "arrayOfStrings",
            "arrayOfEnums"),
        actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    assertEquals(BigDecimal.valueOf(3.14159), actual.getAttributeValue("pi"));
    assertEquals(Boolean.TRUE, actual.getAttributeValue("booly"));
    assertEquals("enumValue2", actual.getAttributeValue("enum"));
    assertEquals(
        List.of(BigDecimal.valueOf(1.2), BigDecimal.valueOf(3.4)),
        actual.getAttributeValue("arrayOfNumbers"));
    assertEquals(List.of("one", "two", "three"), actual.getAttributeValue("arrayOfStrings"));
    assertEquals(List.of("enumValue2", "enumValue1"), actual.getAttributeValue("arrayOfEnums"));
  }

  // arguments for parameterized test, in the form of: input value, input datatype, expected return
  // value
  static Stream<Arguments> provideConvertScalarAttributesArgs() {
    return Stream.of(
        // most basic case
        Arguments.of("hello", Schema.Type.STRING, "hello"),
        // null inputs
        Arguments.of(null, Schema.Type.STRING, null),
        Arguments.of(null, Schema.Type.BOOLEAN, null),
        // numbers
        Arguments.of(Long.MIN_VALUE, Schema.Type.LONG, BigDecimal.valueOf(Long.MIN_VALUE)),
        Arguments.of(Long.MAX_VALUE, Schema.Type.LONG, BigDecimal.valueOf(Long.MAX_VALUE)),
        Arguments.of(Integer.MIN_VALUE, Schema.Type.INT, BigDecimal.valueOf(Integer.MIN_VALUE)),
        Arguments.of(Integer.MAX_VALUE, Schema.Type.INT, BigDecimal.valueOf(Integer.MAX_VALUE)),
        Arguments.of(Float.MIN_VALUE, Schema.Type.FLOAT, BigDecimal.valueOf(Float.MIN_VALUE)),
        Arguments.of(Float.MAX_VALUE, Schema.Type.FLOAT, BigDecimal.valueOf(Float.MAX_VALUE)),
        Arguments.of(Double.MIN_VALUE, Schema.Type.DOUBLE, BigDecimal.valueOf(Double.MIN_VALUE)),
        Arguments.of(Double.MAX_VALUE, Schema.Type.DOUBLE, BigDecimal.valueOf(Double.MAX_VALUE)),
        // booleans
        Arguments.of(true, Schema.Type.BOOLEAN, true),
        Arguments.of(false, Schema.Type.BOOLEAN, false),

        // mismatched inputs and datatypes - will respect the actual value, not the declared type.
        // These should never happen in practice, unless the Avro API has a bug.
        Arguments.of(3.14, Schema.Type.BOOLEAN, BigDecimal.valueOf(3.14)),
        Arguments.of(true, Schema.Type.LONG, true));
  }

  // targeted test for converting scalar Avro values to WDS values
  @ParameterizedTest(name = "with input of {0} and {1}, return value should be {2}")
  @MethodSource("provideConvertScalarAttributesArgs")
  void convertScalarAttributes(Object input, Schema.Type schemaType, Object expected) {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Schema.Field field = new Schema.Field("someFieldName", Schema.create(schemaType));

    Object actual = pfbRecordConverter.convertAttributeType(input, field);
    assertEquals(expected, actual);
  }

  // targeted test for converting scalar Avro enums to WDS values
  @Test
  void convertScalarEnums() {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Schema enumSchema = Schema.createEnum("name", "doc", "namespace", List.of("foo", "bar", "baz"));

    Schema.Field field = new Schema.Field("someFieldName", enumSchema);

    Object input = new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bar");

    Object actual = pfbRecordConverter.convertAttributeType(input, field);
    assertEquals("bar", actual);
  }

  // arguments for parameterized test, in the form of: input value, input datatype, expected return
  // value
  static Stream<Arguments> provideConvertArrayAttributesArgs() {
    return Stream.of(
        // most basic case
        Arguments.of(List.of("hello", "world"), Schema.Type.STRING, List.of("hello", "world")),
        // null inputs
        Arguments.of(null, Schema.Type.STRING, null),
        Arguments.of(null, Schema.Type.BOOLEAN, null),
        // empty arrays
        Arguments.of(List.of(), Schema.Type.STRING, List.of()),
        Arguments.of(List.of(), Schema.Type.LONG, List.of()),
        Arguments.of(List.of(), Schema.Type.INT, List.of()),
        // numbers
        Arguments.of(
            List.of(Long.MIN_VALUE, 1L, Long.MAX_VALUE),
            Schema.Type.LONG,
            List.of(
                BigDecimal.valueOf(Long.MIN_VALUE),
                BigDecimal.valueOf(1L),
                BigDecimal.valueOf(Long.MAX_VALUE))),
        Arguments.of(
            List.of(Integer.MIN_VALUE, 1, Integer.MAX_VALUE),
            Schema.Type.INT,
            List.of(
                BigDecimal.valueOf(Integer.MIN_VALUE),
                BigDecimal.valueOf(1),
                BigDecimal.valueOf(Integer.MAX_VALUE))),
        Arguments.of(
            List.of(Float.MIN_VALUE, 1F, Float.MAX_VALUE),
            Schema.Type.FLOAT,
            List.of(
                BigDecimal.valueOf(Float.MIN_VALUE),
                BigDecimal.valueOf(1F),
                BigDecimal.valueOf(Float.MAX_VALUE))),
        Arguments.of(
            List.of(Double.MIN_VALUE, 1D, Double.MAX_VALUE),
            Schema.Type.DOUBLE,
            List.of(
                BigDecimal.valueOf(Double.MIN_VALUE),
                BigDecimal.valueOf(1D),
                BigDecimal.valueOf(Double.MAX_VALUE))),
        // booleans
        Arguments.of(List.of(true, false, true), Schema.Type.BOOLEAN, List.of(true, false, true)));
  }

  // targeted test for converting array Avro values to WDS values
  @ParameterizedTest(name = "with input of {0} and item type {1}, return value should be {2}")
  @MethodSource("provideConvertArrayAttributesArgs")
  void convertArrayAttributes(Object input, Schema.Type itemType, Object expected) {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Schema arraySchema = Schema.createArray(Schema.create(itemType));

    Schema.Field field = new Schema.Field("someFieldName", arraySchema);

    Object actual = pfbRecordConverter.convertAttributeType(input, field);
    assertEquals(expected, actual);
  }

  // targeted test for converting array Avro enums to WDS values
  @Test
  void convertArrayOfEnums() {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();

    Schema enumSchema = Schema.createEnum("name", "doc", "namespace", List.of("foo", "bar", "baz"));

    Schema.Field field = new Schema.Field("someFieldName", enumSchema);

    Object input =
        List.of(
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "bar"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "foo"),
            new GenericData.EnumSymbol(Schema.create(Schema.Type.STRING), "baz"));

    Object actual = pfbRecordConverter.convertAttributeType(input, field);
    assertEquals(List.of("bar", "foo", "baz"), actual);
  }

  @Test
  void nullFieldArgument() {
    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter();
    assertThrows(
        PfbParsingException.class, () -> pfbRecordConverter.convertAttributeType("anything", null));
  }
}
