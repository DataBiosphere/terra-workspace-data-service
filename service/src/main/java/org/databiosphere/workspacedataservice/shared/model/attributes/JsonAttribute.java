package org.databiosphere.workspacedataservice.shared.model.attributes;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonAttribute extends ScalarAttribute<JsonNode> {

  public JsonAttribute(JsonNode value) {
    super(value);
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
    return this.value.toString();
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
