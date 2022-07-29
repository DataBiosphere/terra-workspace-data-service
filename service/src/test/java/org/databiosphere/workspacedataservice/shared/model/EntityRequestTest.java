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
public class EntityRequestTest {

    @Autowired
    private ObjectMapper jacksonObjectMapper;

    @Test
    void testJsonDeserialization() throws JsonProcessingException {
        EntityId entityId = new EntityId("test-id");
        EntityType entityType = new EntityType("test-type");
        EntityAttributes entityAttributes = new EntityAttributes(Map.of(
                "foo", "bar",
                "num", 123,
                "bool", true,
                "anotherstring", "hello world"));

        EntityRequest expected = new EntityRequest(entityId, entityType, entityAttributes);

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

        EntityRequest actual = jacksonObjectMapper.readValue(inputJsonString, EntityRequest.class);

        assertEquals(expected, actual,
                "EntityRequest did not deserialize from json as expected.");
    }
}
