package org.databiosphere.workspacedataservice.shared.model;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordTest extends ControlPlaneTestBase {

  @Autowired private ObjectMapper jacksonObjectMapper;

  @Test
  void testRecordIdDeserialization() throws JsonProcessingException {
    String input = "\"my-fancy-id\"";

    String expected = "my-fancy-id";

    String actual = jacksonObjectMapper.readValue(input, String.class);

    assertEquals(expected, actual);
  }

  @Test
  void testRecordTypeDeserialization() throws JsonProcessingException {
    String input = "\"my-record-type\"";

    RecordType expected = RecordType.valueOf("my-record-type");

    RecordType actual = jacksonObjectMapper.readValue(input, RecordType.class);

    assertEquals(expected, actual);
  }

  @Test
  void testRecordTypeValidationOnCreation() {
    List<String> invalidNames =
        Arrays.asList(RESERVED_NAME_PREFIX + "anything", "semi;colon", "back\\slash", "bang!");

    invalidNames.forEach(
        testCase -> {
          // Should throw an error
          Exception ex =
              assertThrows(
                  InvalidNameException.class,
                  () -> RecordType.valueOf(testCase),
                  "Exception should be thrown when creating a RecordType of '" + testCase + "'");

          assertTrue(
              ex.getMessage().contains(RESERVED_NAME_PREFIX),
              "Exception message should contain 'sys_'. Was: '['" + ex.getMessage() + "']'");
          assertTrue(
              ex.getMessage().startsWith(InvalidNameException.NameType.RECORD_TYPE.getName()),
              "Exception message should start with 'Record Type'. Was: '['"
                  + ex.getMessage()
                  + "']'");
        });
  }

  @Test
  void testRecordAttributesDeserialization() throws JsonProcessingException {
    RecordAttributes expected =
        new RecordAttributes(
            Map.of(
                "foo",
                "bar",
                "num",
                new BigInteger("123"),
                "bool",
                true,
                "anotherstring",
                "hello world"));

    String inputJsonString =
        """
				{
					"foo": "bar",
					"num": 123,
					"bool": true,
					"anotherstring": "hello world"
				}""";

    RecordAttributes actual =
        jacksonObjectMapper.readValue(inputJsonString, RecordAttributes.class);

    assertEquals(expected, actual, "Record attributes did not match.");
  }

  @Test
  void testRecordDeserialization() throws JsonProcessingException {
    RecordAttributes recordAttributes =
        new RecordAttributes(
            Map.of(
                "foo",
                "bar",
                "num",
                new BigInteger("123"),
                "bool",
                true,
                "anotherstring",
                "hello world"));

    RecordType recordType = RecordType.valueOf("mytype");
    String recordId = "my-id";

    Record expected = new Record(recordId, recordType, recordAttributes);

    String inputJsonString =
        """
				{
				  "id": "my-id",
				  "type": "mytype",
				  "attributes": {
				    "foo": "bar",
				    "num": 123,
				    "bool": true,
				    "anotherstring": "hello world"
				  }
				}""";

    Record actual = jacksonObjectMapper.readValue(inputJsonString, Record.class);

    assertEquals(expected.getId(), actual.getId(), "Record ids did not match.");
    assertEquals(
        expected.getAttributes(), actual.getAttributes(), "Record attributes did not match.");
    assertEquals(expected.getRecordType(), actual.getRecordType(), "Record types did not match.");
  }
}
