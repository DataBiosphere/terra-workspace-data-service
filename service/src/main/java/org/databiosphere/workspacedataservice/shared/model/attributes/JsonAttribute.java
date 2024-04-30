package org.databiosphere.workspacedataservice.shared.model.attributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;

public class JsonAttribute extends ScalarAttribute<JsonNode> {

  @Nullable private final ObjectMapper mapper;

  JsonAttribute(JsonNode value) {
    super(value);
    this.mapper = null;
  }

  JsonAttribute(JsonNode value, ObjectMapper mapper) {
    super(value);
    this.mapper = mapper;
  }

  @Override
  public String toString() {
    // if this JsonAttribute has an ObjectMapper reference, use that ObjectMapper
    // to generate the toString value. Else, use the built-in toString.
    if (mapper == null) {
      return value.toString();
    } else {
      try {
        return mapper.writeValueAsString(this.value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
