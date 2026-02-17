package org.databiosphere.workspacedataservice.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
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

@DirtiesContext
@SpringBootTest(properties = "management.prometheus.metrics.export.enabled=true")
@AutoConfigureMockMvc
class MetricsConfigTest extends ControlPlaneTestBase {
  @Autowired private BuildProperties buildProperties;
  @Autowired private PrometheusMeterRegistry metrics;

  @Test
  void metricsAreTaggedWithVersion() {
    String expectedVersion = buildProperties.getVersion();
    String scrape = metrics.scrape();

    assertThat(scrape).contains("version=\"%s\"".formatted(expectedVersion));
    assertThat(scrape).contains("service=\"wds\"");
  }

  private static Set<String> allowedPrefixes() {
    return MetricsConfig.ALLOWED_PREFIXES;
  }

  @ParameterizedTest(name = "metrics with a \"{0}\" prefix are included in prometheus output")
  @MethodSource("allowedPrefixes")
  void allowlistedMetricsAreVisible(String prefix) {
    String randomMetricName = RandomStringUtils.insecure().nextAlphanumeric(10);
    String expectedPrometheusString = "%s_%s".formatted(prefix, randomMetricName);

    assertThat(metrics.scrape()).doesNotContain(expectedPrometheusString);

    metrics.counter("%s.%s".formatted(prefix, randomMetricName)).increment();

    assertThat(metrics.scrape()).contains(expectedPrometheusString);
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
  void excludeMetricsThatAreNotOnAllowlist(String disallowedMetricName) {
    String unexpectedPrometheusString = disallowedMetricName.replace('.', '_');

    metrics.counter(disallowedMetricName).increment();

    String scrape = metrics.scrape();

    assertThat(scrape).doesNotContain(unexpectedPrometheusString);
  }
}
