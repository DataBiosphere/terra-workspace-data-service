package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.Test;

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
            "objectSchema",
            "doc",
            "namespace",
            false,
            List.of(
                new Schema.Field("marco", Schema.create(Schema.Type.STRING)),
                new Schema.Field("pi", Schema.create(Schema.Type.LONG)),
                new Schema.Field("afile", Schema.create(Schema.Type.STRING))));

    GenericData.Record objectAttributes = new GenericData.Record(myObjSchema);
    objectAttributes.put("marco", "polo");
    objectAttributes.put("pi", 3.14159);
    objectAttributes.put("afile", "https://some/path/to/a/file");

    GenericRecord input =
        PfbTestUtils.makeRecord("this-record-has", "a-null-for-the-object-field", objectAttributes);
    Record actual = new PfbRecordConverter().genericRecordToRecord(input);

    Set<Map.Entry<String, Object>> actualAttributeSet = actual.attributeSet();
    Set<String> actualKeySet =
        actualAttributeSet.stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    assertEquals(Set.of("marco", "pi", "afile"), actualKeySet);

    assertEquals("polo", actual.getAttributeValue("marco"));
    assertEquals("https://some/path/to/a/file", actual.getAttributeValue("afile"));
    // TODO AJ-1452: this should remain a number, not become a string
    assertEquals("3.14159", actual.getAttributeValue("pi"));

    // TODO AJ-1452: add more test coverage as the runtime functionality evolves.
  }
}
