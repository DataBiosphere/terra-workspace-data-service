package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.databiosphere.workspacedataservice.generated.CapabilitiesServerModel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.test.web.servlet.MvcResult;

class CapabilitiesControllerTest extends MockMvcTestBase {

  @Value("classpath:capabilities.json")
  Resource capabilitiesResource;

  @Test
  void resourceFileIsValid() {
    assertDoesNotThrow(
        () -> fromJson(capabilitiesResource.getInputStream(), CapabilitiesServerModel.class),
        "Have you modified capabilities.json? Is it still valid JSON?");
  }

  @Test
  void restResponseIsValid() throws Exception {
    MvcResult mvcResult =
        mockMvc.perform(get("/capabilities/v1")).andExpect(status().isOk()).andReturn();

    // is the response parsable into the capabilities model?
    CapabilitiesServerModel actual =
        assertDoesNotThrow(() -> fromJson(mvcResult, CapabilitiesServerModel.class));
    // is the response non-empty?
    assertThat(actual.getAdditionalProperties()).isNotEmpty();
  }
}
