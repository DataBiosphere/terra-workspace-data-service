package org.databiosphere.workspacedataservice.shared.model.attributes;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonAttribute extends ScalarAttribute<JsonNode> {

  private final ObjectMapper mapper;

  public JsonAttribute(JsonNode value, ObjectMapper mapper) {
    super(value);
    this.mapper = mapper;
  }

  /**
   * When serializing the JsonAttribute class, what value should Jackson serialize? We should only
   * serialize the underlying value, not the JsonAttribute wrapper itself.
   *
   * @return the value to use when serializing
   */
  @JsonValue
  public JsonNode jsonValue() {
    return this.value;
  }

  @Override
  public String toString() {
    // if this JsonAttribute has an ObjectMapper reference, use that ObjectMapper
    // to generate the toString value. Else, use the built-in toString.
    try {
      return mapper.writeValueAsString(this.value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
