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
    // use the supplied ObjectMapper to generate the toString value. This ensures serialization is
    // consistent with the settings configured on the ObjectMapper.
    try {
      return mapper.writeValueAsString(this.value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    // don't consider the mapper in equals
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    // don't consider the mapper in hashcode
    return super.hashCode();
  }
}
