package org.databiosphere.workspacedataservice.service;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Collections.emptyList;
import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.RelationUtils.createRelationString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DataTypeInfererTest extends ControlPlaneTestBase {

  @Autowired DataTypeInferer inferer;

  @Test
  void inferTypesJsonSource() {
    Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeAttrs());
    Map<String, DataTypeMapping> expected = new HashMap<>();
    expected.put("array_of_string", DataTypeMapping.ARRAY_OF_STRING);
    expected.put("string_val", DataTypeMapping.STRING);
    expected.put("int_val", DataTypeMapping.NUMBER);
    expected.put("json_val", DataTypeMapping.JSON);
    expected.put("date_val", DataTypeMapping.DATE);
    expected.put("date_time_val", DataTypeMapping.DATE_TIME);
    expected.put("file_val", DataTypeMapping.FILE);
    expected.put("number_or_string", DataTypeMapping.STRING);
    expected.put("relation", DataTypeMapping.RELATION);
    expected.put("rel_arr", DataTypeMapping.ARRAY_OF_RELATION);
    expected.put("array_of_file", DataTypeMapping.ARRAY_OF_FILE);
    assertEquals(expected, result);
  }

  @Test
  void selectBestTypes() {
    assertThat(inferer.selectBestType(DataTypeMapping.NULL, DataTypeMapping.NUMBER))
        .as("null values should not affect typing for non null values")
        .isEqualTo(DataTypeMapping.NUMBER);
    assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.STRING))
        .as("if types are identical, return the type")
        .isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.BOOLEAN))
        .as("should generalize to string/text type")
        .isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.selectBestType(DataTypeMapping.DATE, DataTypeMapping.DATE_TIME))
        .as("should convert date to datetime")
        .isEqualTo(DataTypeMapping.DATE_TIME);
    assertThat(
            inferer.selectBestType(
                DataTypeMapping.ARRAY_OF_DATE, DataTypeMapping.ARRAY_OF_DATE_TIME))
        .as("should convert array of date to array of datetime")
        .isEqualTo(DataTypeMapping.ARRAY_OF_DATE_TIME);
    assertThat(
            inferer.selectBestType(
                DataTypeMapping.ARRAY_OF_STRING, DataTypeMapping.ARRAY_OF_RELATION))
        .as("should convert array of relation to array of string")
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
    assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.RELATION))
        .as("should convert relation to string")
        .isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.selectBestType(DataTypeMapping.FILE, DataTypeMapping.STRING))
        .as("should convert file to string")
        .isEqualTo(DataTypeMapping.STRING);
    assertThat(
            inferer.selectBestType(DataTypeMapping.ARRAY_OF_FILE, DataTypeMapping.ARRAY_OF_STRING))
        .as("should convert array of file to array of string")
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
  }

  @Test
  void isValidJson() {
    assertThat(inferer.tryJsonObject(RandomStringUtils.randomNumeric(10))).isEmpty();
    assertThat(inferer.tryJsonObject("Hello")).isEmpty();
    assertThat(inferer.tryJsonObject(Boolean.TRUE.toString())).isEmpty();
    assertThat(inferer.tryJsonObject("True")).isEmpty();
    assertThat(inferer.tryJsonObject("{\"foo\":\"bar\"}")).isPresent();
  }

  @Test
  void nullValuesMixedWithNonNullsShouldStillYieldProperTypeJSON() {
    RecordAttributes firstAttrs =
        RecordAttributes.empty().putAttribute("boolean", null).putAttribute("long", null);
    Record first = new Record("first", RecordType.valueOf("test-inference"), firstAttrs);
    RecordAttributes secondAttrs =
        RecordAttributes.empty()
            .putAttribute("boolean", "true")
            .putAttribute("long", new BigInteger("-999999"));
    Record second = new Record("second", RecordType.valueOf("test-inference"), secondAttrs);
    Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(List.of(first, second));
    assertThat(inferredSchema)
        .as("Should still get BOOLEAN and LONG for types despite null values in one record")
        .isEqualTo(Map.of("boolean", DataTypeMapping.BOOLEAN, "long", DataTypeMapping.NUMBER));
  }

  @Test
  void inferSomeTypes() {
    assertThat(inferer.inferType("True")).isEqualTo(DataTypeMapping.BOOLEAN);
    assertThat(inferer.inferType("Hello")).isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.inferType("2020-01-01")).isEqualTo(DataTypeMapping.DATE);
    assertThat(inferer.inferType("2020-01-01T00:10:00")).isEqualTo(DataTypeMapping.DATE_TIME);
    assertThat(inferer.inferType("2020-01-01T00:10:00")).isEqualTo(DataTypeMapping.DATE_TIME);
    assertThat(
            inferer.inferType(
                "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/my%20file.pdf"))
        .isEqualTo(DataTypeMapping.FILE);
    assertThat(inferer.inferType("12345")).isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.inferType("12345A")).isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.inferType(List.of("Hello!"))).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
    assertThat(inferer.inferType(List.of(new BigInteger("12345"))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_NUMBER);
    assertThat(inferer.inferType(List.of(true, false, true)))
        .isEqualTo(DataTypeMapping.ARRAY_OF_BOOLEAN);
    assertThat(
            inferer.inferType(
                List.of(new BigDecimal("11.1"), new BigDecimal("12"), new BigDecimal("14"))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_NUMBER);
    assertThat(
            inferer.inferType(
                List.of(
                    createRelationString(RecordType.valueOf("testType"), "recordId"),
                    createRelationString(RecordType.valueOf("testType"), "recordId2"),
                    createRelationString(RecordType.valueOf("testType"), "recordId3"))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_RELATION);
    assertThat(
            inferer.inferType(
                List.of(
                    createRelationString(RecordType.valueOf("testType"), "recordId"),
                    "not a relation string",
                    createRelationString(RecordType.valueOf("testType"), "recordId3"))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
    assertThat(inferer.inferType(createRelationString(RecordType.valueOf("testType"), "recordId3")))
        .isEqualTo(DataTypeMapping.RELATION);
  }

  @Test
  void ambiguousConversions() {
    assertThat(inferer.inferType(List.of(true, "false", "True")))
        .isEqualTo(DataTypeMapping.ARRAY_OF_BOOLEAN);
    assertThat(inferer.inferType(List.of("11", "99"))).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
    assertThat(inferer.inferType(List.of("11", new BigDecimal("99"), "foo")))
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
    assertThat(inferer.inferType("")).isEqualTo(DataTypeMapping.STRING);
    assertThat(
            inferer.inferType(
                List.of(new BigInteger("11"), new BigInteger("99"), new BigDecimal("-3.14"))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_NUMBER);
    assertThat(
            inferer.inferType(
                List.of(new BigInteger("11"), new BigInteger("99"), new BigDecimal("-3.14"), "09")))
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
    assertThat(inferer.inferType(emptyList())).isEqualTo(DataTypeMapping.EMPTY_ARRAY);
    assertThat(inferer.inferType("[11, 99, -3.14, 09]")).isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.inferType("[a]")).isEqualTo(DataTypeMapping.STRING);
    assertThat(inferer.inferType("[11, 99, -3.14, 09]")).isEqualTo(DataTypeMapping.STRING);
  }

  @Test
  void inferArraysOfOnlyNulls() {
    assertThat(inferer.inferType(Arrays.asList(null, null, null)))
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
  }

  @Test
  void inferArraysOfSomeNulls() {
    assertThat(inferer.inferType(Arrays.asList(null, "foo", null, "bar", null)))
        .isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
  }

  @Test
  void inferArraysOfJson() {
    // array of json objects
    assertThat(inferer.inferType(Arrays.asList(Map.of("foo", "bar"), Map.of("num", 42))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_JSON);
    // array of nested json objects
    assertThat(
            inferer.inferType(
                Arrays.asList(
                    Map.of("foo", Map.of("one", "two")), Map.of("bar", Map.of("three", "four")))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_JSON);
  }

  @Test
  void inferMixedArrays() {
    // array of mixed values, including a json object
    assertThat(
            inferer.inferType(
                Arrays.asList(Map.of("foo", "bar"), new BigInteger("11"), "I'm a string")))
        .isEqualTo(DataTypeMapping.ARRAY_OF_JSON);
  }

  @Test
  void inferNestedArrays() {
    // nested array of numbers
    assertThat(
            inferer.inferType(
                Arrays.asList(List.of(1), Arrays.asList(2, 3), Arrays.asList(4, 5, 6))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_JSON);
    // nested array of strings
    assertThat(
            inferer.inferType(
                Arrays.asList(
                    List.of("one"),
                    Arrays.asList("two", "three"),
                    Arrays.asList("four", "five", "six"))))
        .isEqualTo(DataTypeMapping.ARRAY_OF_JSON);
  }

  // Test for [AJ-1143]: TSV fails to upload if it has nulls in a relation column
  @Test
  void allowNullRelations() {
    List<Record> records =
        List.of(
            new Record(
                "1",
                RecordType.valueOf("thing"),
                RecordAttributes.empty().putAttribute("rel", null).putAttribute("str", "hello")),
            new Record(
                "1",
                RecordType.valueOf("thing"),
                RecordAttributes.empty()
                    .putAttribute("rel", createRelationString(RecordType.valueOf("thing"), "1"))
                    .putAttribute("str", "world")));
    Map<String, DataTypeMapping> schema = inferer.inferTypes(records);

    Set<Relation> relations = inferer.findRelations(records, schema).relations();
    assertThat(relations).hasSize(1);
    Relation relation = relations.stream().collect(onlyElement());
    assertThat(relation.relationColName()).isEqualTo("rel");
    assertThat(relation.relationRecordType()).isEqualTo(RecordType.valueOf("thing"));
  }

  @Test
  void allowNullMultivalueRelations() {
    List<Record> records =
        List.of(
            new Record(
                "1",
                RecordType.valueOf("thing"),
                RecordAttributes.empty().putAttribute("rel", null).putAttribute("str", "hello")),
            new Record(
                "2",
                RecordType.valueOf("thing"),
                RecordAttributes.empty()
                    .putAttribute(
                        "rel", List.of(createRelationString(RecordType.valueOf("thing"), "1")))
                    .putAttribute("str", "world")));
    Map<String, DataTypeMapping> schema = inferer.inferTypes(records);

    Set<Relation> relationArrays = inferer.findRelations(records, schema).relationArrays();
    assertThat(relationArrays).hasSize(1);
    Relation relation = relationArrays.stream().collect(onlyElement());
    assertThat(relation.relationColName()).isEqualTo("rel");
    assertThat(relation.relationRecordType()).isEqualTo(RecordType.valueOf("thing"));
  }

  private static RecordAttributes getSomeAttrs() {
    return new RecordAttributes(
        ofEntries(
            entry("int_val", new BigDecimal("4747")),
            entry("string_val", "Abracadabra Open Sesame"),
            entry("json_val", "{\"list\": [\"a\", \"b\"]}"),
            entry("date_val", "2001-11-03"),
            entry("date_time_val", "2001-11-03T10:00:00"),
            entry("number_or_string", "47"),
            entry("array_of_string", List.of("red", "yellow")),
            entry(
                "relation",
                createRelationString(RecordType.valueOf("testRecordType"), "testRecordId")),
            entry(
                "rel_arr",
                List.of(
                    createRelationString(RecordType.valueOf("testRecordType"), "testRecordId"),
                    createRelationString(RecordType.valueOf("testRecordType"), "testRecordId2"),
                    createRelationString(RecordType.valueOf("testRecordType"), "testRecordId3"))),
            entry(
                "file_val",
                "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/file.cram?param=foo"),
            entry(
                "array_of_file",
                List.of(
                    "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/notebook.ipynb",
                    "drs://jade.datarepo-dev.broadinstitute.org/v1_9545e956-aa6a-4b84-a037-d0ed164c1890"))));
  }
}
