package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@AutoConfigureMockMvc
@SpringBootTest
class MockMvcTestBase extends ControlPlaneTestBase {
  @Autowired private ObjectMapper mapper;
  @Autowired protected MockMvc mockMvc;

  protected String toJson(Object value) throws JsonProcessingException {
    return mapper.writeValueAsString(value);
  }

  protected <T> T fromJson(MvcResult result, Class<T> valueType)
      throws UnsupportedEncodingException, JsonProcessingException {
    return mapper.readValue(result.getResponse().getContentAsString(), valueType);
  }

  protected <T> T fromJson(InputStream inputStream, Class<T> valueType) throws IOException {
    return mapper.readValue(inputStream, valueType);
  }
}
