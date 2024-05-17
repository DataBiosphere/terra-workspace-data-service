package org.databiosphere.workspacedataservice.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class JsonUtils implements InitializingBean {
  private static JsonUtils instance;

  private final ObjectMapper objectMapper;

  public JsonUtils(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void afterPropertiesSet() {
    instance = this;
  }

  public static JsonUtils getInstance() {
    return instance;
  }

  public static String stringify(Object value) {
    try {
      return getInstance().objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing JSON", e);
    }
  }

  public static <T> T parse(String string, Class<T> valueType) {
    try {
      return getInstance().objectMapper.readValue(string, valueType);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing JSON", e);
    }
  }
}
