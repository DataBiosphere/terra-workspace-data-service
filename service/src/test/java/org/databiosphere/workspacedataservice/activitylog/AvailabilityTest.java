package org.databiosphere.workspacedataservice.activitylog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

@AutoConfigureMockMvc
@SpringBootTest
class AvailabilityTest extends ControlPlaneTestBase {

  @Autowired private MockMvc mvc;
  @Autowired private ApplicationContext context;
  @Autowired private ApplicationAvailability applicationAvailability;

  // Reference: https://www.baeldung.com/spring-liveness-readiness-probes
  @Test
  void readinessState() throws Exception {
    assertThat(applicationAvailability.getReadinessState())
        .isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);
    ResultActions readinessResult = mvc.perform(get("/status/readiness"));
    readinessResult.andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));

    AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);
    assertThat(applicationAvailability.getReadinessState())
        .isEqualTo(ReadinessState.REFUSING_TRAFFIC);
    mvc.perform(get("/status/readiness"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value("OUT_OF_SERVICE"));
  }

  @Test
  void livenessState() throws Exception {
    assertThat(applicationAvailability.getLivenessState()).isEqualTo(LivenessState.CORRECT);
    ResultActions result = mvc.perform(get("/status/liveness"));
    result.andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
    ResultActions livenessResult = mvc.perform(get("/status/liveness"));
    livenessResult.andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));

    AvailabilityChangeEvent.publish(context, LivenessState.BROKEN);
    assertThat(applicationAvailability.getLivenessState()).isEqualTo(LivenessState.BROKEN);
    mvc.perform(get("/status/liveness"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value("DOWN"));
  }
}
