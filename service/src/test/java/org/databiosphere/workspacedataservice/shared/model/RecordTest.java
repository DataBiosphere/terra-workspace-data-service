package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordTest {

	@Autowired
	private ObjectMapper jacksonObjectMapper;

	@Test
	void testRecordIdDeserialization() throws JsonProcessingException {
		String input = "\"my-fancy-id\"";

		RecordId expected = new RecordId("my-fancy-id");

		RecordId actual = jacksonObjectMapper.readValue(input, RecordId.class);

		assertEquals(expected, actual);
	}

	@Test
	void testRecordTypeDeserialization() throws JsonProcessingException {
		String input = "\"my-record-type\"";

		RecordType expected = RecordType.forUnitTest("my-record-type");

		RecordType actual = jacksonObjectMapper.readValue(input, RecordType.class);

		assertEquals(expected, actual);
	}

	@Test
	void testRecordAttributesDeserialization() throws JsonProcessingException {
		RecordAttributes expected = new RecordAttributes(
				Map.of("foo", "bar", "num", 123, "bool", true, "anotherstring", "hello world"));

		String inputJsonString = """
				{
					"foo": "bar",
					"num": 123,
					"bool": true,
					"anotherstring": "hello world"
				}""";

		RecordAttributes actual = jacksonObjectMapper.readValue(inputJsonString, RecordAttributes.class);

		assertEquals(expected, actual, "Record attributes did not match.");
	}

	@Test
	void testRecordDeserialization() throws JsonProcessingException {
		RecordAttributes recordAttributes = new RecordAttributes(
				Map.of("foo", "bar", "num", 123, "bool", true, "anotherstring", "hello world"));

		RecordType recordType = RecordType.forUnitTest("mytype");
		RecordId recordId = new RecordId("my-id");

		Record expected = new Record(recordId, recordType, recordAttributes);

		String inputJsonString = """
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
		assertEquals(expected.getAttributes(), actual.getAttributes(), "Record attributes did not match.");
		assertEquals(expected.getRecordType(), actual.getRecordType(), "Record types did not match.");
	}
}
