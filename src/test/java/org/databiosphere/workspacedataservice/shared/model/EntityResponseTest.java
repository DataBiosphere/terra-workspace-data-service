package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StringUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityResponseTest {

    @Autowired
    private ObjectMapper jacksonObjectMapper;

    @Test
    void testJsonSerialization() throws JsonProcessingException {
        EntityId entityId = new EntityId("test-id");
        EntityType entityType = new EntityType("test-type");
        EntityAttributes entityAttributes = new EntityAttributes(Map.of(
                "foo", "bar",
                "num", 123,
                "bool", true,
                "anotherstring", "hello world"));
        EntityMetadata entityMetadata = new EntityMetadata("test-provenance");

        EntityResponse entityResponse = new EntityResponse(entityId, entityType, entityAttributes, entityMetadata);

        String actual = jacksonObjectMapper.writeValueAsString(entityResponse);

        // N.B. keys inside attributes will be sorted
        String expectedJsonString = """
                {
                  "id": "test-id",
                  "type": "test-type",
                  "attributes": {
                    "anotherstring": "hello world",
                    "bool": true,
                    "foo": "bar",
                    "num": 123
                  },
                  "metadata": {
                    "provenance": "test-provenance"
                  }
                }""";

        // compare, ignoring whitespace
        assertEquals(StringUtils.trimAllWhitespace(expectedJsonString),
                StringUtils.trimAllWhitespace(actual),
                "EntityResponse did not serialize to json as expected.");
    }
}
