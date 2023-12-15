package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.generated.CapabilitiesServerModel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@DirtiesContext
@SpringBootTest
@AutoConfigureMockMvc
class CapabilitiesControllerTest {
  @Autowired MockMvc mockMvc;
  @Autowired ObjectMapper objectMapper;

  @Value("classpath:capabilities.json")
  Resource capabilitiesResource;

  @Test
  void resourceFileIsValid() {
    assertDoesNotThrow(
        () ->
            objectMapper.readValue(
                capabilitiesResource.getInputStream(), CapabilitiesServerModel.class),
        "Have you modified capabilities.json? Is it still valid JSON?");
  }

  @Test
  void restResponseIsValid() throws Exception {
    MvcResult mvcResult =
        mockMvc.perform(get("/capabilities/v1")).andExpect(status().isOk()).andReturn();

    String rawResponse = mvcResult.getResponse().getContentAsString();

    // is the response parsable into the capabilities model?
    CapabilitiesServerModel actual =
        assertDoesNotThrow(
            () -> objectMapper.readValue(rawResponse, CapabilitiesServerModel.class));
    // is the response non-empty?
    assertThat(actual).isNotEmpty();
  }
}
