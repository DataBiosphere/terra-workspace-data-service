package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordRequestTest {

	@Autowired
	private ObjectMapper jacksonObjectMapper;

	@Test
	void testJsonDeserialization() throws JsonProcessingException {
		RecordId recordId = new RecordId("test-id");
		RecordType recordType = new RecordType("test-type");
		RecordAttributes recordAttributes = new RecordAttributes(
				Map.of("foo", "bar", "num", 123, "bool", true, "anotherstring", "hello world"));

		RecordRequest expected = new RecordRequest(recordId, recordType, recordAttributes);

		String inputJsonString = """
				{
				  "id": "test-id",
				  "type": "test-type",
				  "attributes": {
				    "foo": "bar",
				    "num": 123,
				    "bool": true,
				    "anotherstring": "hello world"
				  }
				}""";

		RecordRequest actual = jacksonObjectMapper.readValue(inputJsonString, RecordRequest.class);

		assertEquals(expected, actual, "RecordRequest did not deserialize from json as expected.");
	}
}
