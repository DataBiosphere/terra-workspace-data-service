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

  /**
   * Do not rely on toString() to return valid serialized JSON. Instead, call getValue() or
   * sqlValue() to return a JsonNode, and serialize that yourself via your own ObjectMapper.
   *
   * @return a string representation of this JsonAttribute
   */
  @Override
  public String toString() {
    /* This calls JsonNode.toString(), which "will produce ... valid JSON using default settings"
       and therefore can be DIFFERENT from the JSON produced by calling ObjectMapper.writeValueAsString
       if the ObjectMapper has been configured with non-default flags. Such is the case in WDS; we
       configure non-default flags in JsonConfig.

       Because of these subtle differences in serialization, this toString() intentionally includes
       a "JsonAttribute(...)" text surrounding the JSON value. This should be a breaking signal to
       any callers who try to get JSON out of toString().
    */
    return "JsonAttribute(%s)".formatted(this.value.toString());
  }
}
