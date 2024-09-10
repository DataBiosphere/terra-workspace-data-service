package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.util.Map;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordRequestTest extends ControlPlaneTestBase {

  @Autowired private ObjectMapper jacksonObjectMapper;

  @Test
  void testJsonDeserialization() throws JsonProcessingException {
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

    RecordRequest expected = new RecordRequest(recordAttributes);

    String inputJsonString =
        """
				{
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
