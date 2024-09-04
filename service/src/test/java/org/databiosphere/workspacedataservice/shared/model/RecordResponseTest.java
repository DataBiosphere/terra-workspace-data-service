package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StringUtils;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordResponseTest extends ControlPlaneTestBase {

  @Autowired private ObjectMapper jacksonObjectMapper;

  @Test
  void testJsonSerialization() throws JsonProcessingException {
    String recordId = "test-id";
    RecordType recordType = RecordType.valueOf("test-type");
    RecordAttributes recordAttributes =
        new RecordAttributes(
            Map.of("foo", "bar", "num", 123, "bool", true, "anotherstring", "hello world"));

    RecordResponse recordResponse = new RecordResponse(recordId, recordType, recordAttributes);

    String actual = jacksonObjectMapper.writeValueAsString(recordResponse);

    // N.B. keys inside attributes will be sorted
    String expectedJsonString =
        """
				{
				  "id": "test-id",
				  "type": "test-type",
				  "attributes": {
				    "anotherstring": "hello world",
				    "bool": true,
				    "foo": "bar",
				    "num": 123
				  }
				}""";

    // compare, ignoring whitespace
    assertEquals(
        StringUtils.trimAllWhitespace(expectedJsonString),
        StringUtils.trimAllWhitespace(actual),
        "RecordResponse did not serialize to json as expected.");
  }
}
