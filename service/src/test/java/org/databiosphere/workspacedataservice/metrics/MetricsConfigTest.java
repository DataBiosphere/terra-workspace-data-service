package org.databiosphere.workspacedataservice.metrics;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@DirtiesContext
@SpringBootTest(properties = "management.prometheus.metrics.export.enabled=true")
@AutoConfigureMockMvc
class MetricsConfigTest extends ControlPlaneTestBase {
  @Autowired private BuildProperties buildProperties;
  @Autowired private MockMvc mockMvc;
  @Autowired private MeterRegistry metrics;

  @Test
  void metricsAreTaggedWithVersion() throws Exception {
    String expectedVersion = buildProperties.getVersion();
    mockMvc
        .perform(get("/prometheus"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("version=\"%s\"".formatted(expectedVersion))))
        .andExpect(content().string(containsString("service=\"wds\"")));
  }

  private static Set<String> allowedPrefixes() {
    return MetricsConfig.ALLOWED_PREFIXES;
  }

  @ParameterizedTest(name = "metrics with a \"{0}\" prefix are included in prometheus output")
  @MethodSource("allowedPrefixes")
  void allowlistedMetricsAreVisible(String prefix) throws Exception {
    String randomMetricName = RandomStringUtils.randomAlphanumeric(10);
    String expectedPrometheusString = "%s_%s".formatted(prefix, randomMetricName);

    mockMvc
        .perform(get("/prometheus"))
        .andExpect(status().isOk())
        .andExpect(content().string(not(containsString(expectedPrometheusString))));

    metrics.counter("%s.%s".formatted(prefix, randomMetricName)).increment();

    mockMvc
        .perform(get("/prometheus"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString(expectedPrometheusString)));
  }

  @ParameterizedTest(name = "\"{0}\" is excluded from prometheus output")
  @ValueSource(
      strings = {
        "some.random.metric",
        "foo",
        "bar",
        "foobar",
        // wds is an allowed prefix, but doesn't count if it's embedded deeper in the name
        "metric.containing.but.not.starting.with.wds"
      })
  void excludeMetricsThatAreNotOnAllowlist(String disallowedMetricName) throws Exception {
    String unexpectedPrometheusString = disallowedMetricName.replace('.', '_');

    metrics.counter(disallowedMetricName).increment();

    mockMvc
        .perform(get("/prometheus"))
        .andExpect(status().isOk())
        .andExpect(content().string(not(containsString(unexpectedPrometheusString))));
  }
}
