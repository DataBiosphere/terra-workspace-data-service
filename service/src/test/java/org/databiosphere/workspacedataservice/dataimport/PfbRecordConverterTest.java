package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.Test;

class PfbRecordConverterTest {

  // PFB "id" and "name" columns become the WDS Record id and type, respectively
  @Test
  void recordIdentifiers() {
    String inputId = RandomStringUtils.randomAlphanumeric(10);
    String inputName = RandomStringUtils.randomAlphanumeric(16);
    GenericRecord input = PfbTestUtils.makeRecord(inputId, inputName);

    Record actual = new PfbRecordConverter(Map.of()).genericRecordToRecord(input);

    assertEquals(inputId, actual.getId());
    assertEquals(inputName, actual.getRecordTypeName());
  }

  // if the GenericRecord has {} in the "object" column, the WDS record should have no attributes
  @Test
  void emptyObjectAttributes() {
    GenericRecord input = PfbTestUtils.makeRecord("my-id", "my-name");
    Record actual = new PfbRecordConverter(Map.of()).genericRecordToRecord(input);

    assertThat(actual.attributeSet()).isEmpty();
  }

  // if the GenericRecord has null in the "object" column, the WDS record should have no attributes
  @Test
  void nullObjectAttributes() {
    GenericRecord input =
        PfbTestUtils.makeRecord("this-record-has", "a-null-for-the-object-field", null);
    Record actual = new PfbRecordConverter(Map.of()).genericRecordToRecord(input);

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
    Record actual = new PfbRecordConverter(wdsSchema).genericRecordToRecord(input);

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
    Record actual = new PfbRecordConverter(wdsSchema).genericRecordToRecord(input);

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
    Record actual = new PfbRecordConverter(wdsSchema).genericRecordToRecord(input);

    Set<Map.Entry<String, Object>> actualAttributeSet = actual.attributeSet();
    Set<String> actualKeySet =
        actualAttributeSet.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertEquals(Set.of("marco", "pi", "afile", "booly"), actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    assertEquals("3.14159", actual.getAttributeValue("pi"));
    assertEquals("true", actual.getAttributeValue("booly"));
  }
}
