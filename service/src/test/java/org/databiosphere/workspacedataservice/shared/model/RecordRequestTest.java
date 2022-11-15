package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigInteger;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordRequestTest {

	@Autowired
	private ObjectMapper jacksonObjectMapper;

	@Test
	void testJsonDeserialization() throws JsonProcessingException {
		RecordAttributes recordAttributes = new RecordAttributes(
				Map.of("foo", "bar", "num", new BigInteger("123"), "bool", true, "anotherstring", "hello world"));

		RecordRequest expected = new RecordRequest(recordAttributes, "row_id");

		String inputJsonString = """
				{
				  "attributes": {
				    "foo": "bar",
				    "num": 123,
				    "bool": true,
				    "anotherstring": "hello world"
				  },
				  "recordTypeRowIdentifier": "row_id"
				}""";

		RecordRequest actual = jacksonObjectMapper.readValue(inputJsonString, RecordRequest.class);

		assertEquals(expected, actual, "RecordRequest did not deserialize from json as expected.");
	}
}
