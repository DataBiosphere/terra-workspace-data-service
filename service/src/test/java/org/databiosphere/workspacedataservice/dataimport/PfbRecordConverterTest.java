package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

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

  // if the GenericRecord has attributes in the "object" column, the WDS record should have the same
  // attributes
  @Test
  void valuesInObjectAttributes() {
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
                new Schema.Field("booly", Schema.create(Schema.Type.BOOLEAN))));

    GenericData.Record objectAttributes = new GenericData.Record(myObjSchema);
    objectAttributes.put("marco", "polo");
    objectAttributes.put("pi", 3.14159);
    objectAttributes.put("afile", "https://some/path/to/a/file");
    objectAttributes.put("booly", Boolean.TRUE);

    // translate the object schema to WDS schema
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Map<String, Map<String, DataTypeMapping>> wdsSchema =
        Map.of("mytype", pfbSchemaConverter.pfbSchemaToWdsSchema(myObjSchema));

    GenericRecord input = PfbTestUtils.makeRecord("my-id", "mytype", objectAttributes);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    Set<Map.Entry<String, Object>> actualAttributeSet = actual.attributeSet();
    Set<String> actualKeySet =
        actualAttributeSet.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertEquals(Set.of("marco", "pi", "afile", "booly"), actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    assertEquals(BigDecimal.valueOf(3.14159), actual.getAttributeValue("pi"));
    assertEquals(Boolean.TRUE, actual.getAttributeValue("booly"));

    // TODO AJ-1452: add more test coverage as the runtime functionality evolves.
  }

  // if the GenericRecord contains attributes that are not mentioned in the expected schema,
  // those attributes should be treated as strings.
  @Test
  void missingFieldsInWdsSchema() {
    Schema myObjSchema =
        Schema.createRecord(
            "mytype",
            "doc",
            "namespace",
            false,
            List.of(
                new Schema.Field("marco", Schema.create(Schema.Type.STRING)),
                new Schema.Field("pi", Schema.create(Schema.Type.LONG)),
                new Schema.Field("e", Schema.create(Schema.Type.LONG)),
                new Schema.Field("booly", Schema.create(Schema.Type.BOOLEAN))));

    GenericData.Record objectAttributes = new GenericData.Record(myObjSchema);
    objectAttributes.put("marco", "polo");
    objectAttributes.put("pi", 3.14159);
    objectAttributes.put("e", 2.718);
    objectAttributes.put("booly", Boolean.TRUE);

    // create a WDS schema that is missing some entries
    Map<String, Map<String, DataTypeMapping>> wdsSchema =
        Map.of("mytype", Map.of("marco", DataTypeMapping.STRING, "pi", DataTypeMapping.NUMBER));

    GenericRecord input = PfbTestUtils.makeRecord("my-id", "mytype", objectAttributes);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    Set<Map.Entry<String, Object>> actualAttributeSet = actual.attributeSet();
    Set<String> actualKeySet =
        actualAttributeSet.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertEquals(Set.of("marco", "pi", "e", "booly"), actualKeySet);

    // these have mappings in the WDS schema
    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals(BigDecimal.valueOf(3.14159), actual.getAttributeValue("pi"));

    // these do not have mappings and thus get .toString()-ed
    assertEquals("2.718", actual.getAttributeValue("e"));
    assertEquals("true", actual.getAttributeValue("booly"));
  }

  // if the GenericRecord contains a record type that is not mentioned in the expected schema,
  // all attributes of that type should be treated as strings.
  @Test
  void missingRecordTypesInWdsSchema() {
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
                new Schema.Field("booly", Schema.create(Schema.Type.BOOLEAN))));

    GenericData.Record objectAttributes = new GenericData.Record(myObjSchema);
    objectAttributes.put("marco", "polo");
    objectAttributes.put("pi", 3.14159);
    objectAttributes.put("afile", "https://some/path/to/a/file");
    objectAttributes.put("booly", Boolean.TRUE);

    // translate the object schema to WDS schema
    PfbSchemaConverter pfbSchemaConverter = new PfbSchemaConverter();
    Map<String, Map<String, DataTypeMapping>> wdsSchema =
        Map.of("THISISTHEWRONGTYPE", pfbSchemaConverter.pfbSchemaToWdsSchema(myObjSchema));

    GenericRecord input = PfbTestUtils.makeRecord("my-id", "mytype", objectAttributes);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    Set<Map.Entry<String, Object>> actualAttributeSet = actual.attributeSet();
    Set<String> actualKeySet =
        actualAttributeSet.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertEquals(Set.of("marco", "pi", "afile", "booly"), actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    assertEquals("3.14159", actual.getAttributeValue("pi"));
    assertEquals("true", actual.getAttributeValue("booly"));
  }

  // arguments for parameterized test, in the form of: input value, input datatype, expected return
  // value
  static Stream<Arguments> provideConvertAttributeTypeArgs() {
    return Stream.of(
        // most basic case
        Arguments.of("hello", DataTypeMapping.STRING, "hello"),
        // null inputs
        Arguments.of(null, DataTypeMapping.STRING, null),
        Arguments.of(null, DataTypeMapping.BOOLEAN, null),
        // numbers
        Arguments.of(Long.MIN_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Long.MIN_VALUE)),
        Arguments.of(Long.MAX_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Long.MAX_VALUE)),
        Arguments.of(
            Integer.MIN_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Integer.MIN_VALUE)),
        Arguments.of(
            Integer.MAX_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Integer.MAX_VALUE)),
        Arguments.of(Float.MIN_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Float.MIN_VALUE)),
        Arguments.of(Float.MAX_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Float.MAX_VALUE)),
        Arguments.of(
            Double.MIN_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Double.MIN_VALUE)),
        Arguments.of(
            Double.MAX_VALUE, DataTypeMapping.NUMBER, BigDecimal.valueOf(Double.MAX_VALUE)),
        // booleans
        Arguments.of(true, DataTypeMapping.BOOLEAN, true),
        Arguments.of(false, DataTypeMapping.BOOLEAN, false),

        // mismatched inputs and datatypes - will be toString()-ed
        Arguments.of(3.14, DataTypeMapping.BOOLEAN, "3.14"),
        Arguments.of(true, DataTypeMapping.NUMBER, "true"),

        // null datatypes - will be toString()-ed
        Arguments.of("hi", null, "hi"),
        Arguments.of(3.14, null, "3.14"),
        Arguments.of(true, null, "true"));
  }

  //  @ParameterizedTest(name = "with input of {0} and {1}, return value should be {2}")
  //  @MethodSource("provideConvertAttributeTypeArgs")
  //  void convertAttributeType(Object input, DataTypeMapping inputDataType, Object expected) {
  //    // schema doesn't matter here for creating the converter
  //    PfbRecordConverter pfbRecordConverter = new PfbRecordConverter(Map.of());
  //
  //    Object actual = pfbRecordConverter.convertAttributeType(input, inputDataType);
  //    assertEquals(expected, actual);
  //  }
}
