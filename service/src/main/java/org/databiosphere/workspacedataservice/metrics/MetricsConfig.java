package org.databiosphere.workspacedataservice.metrics;

import static io.micrometer.core.instrument.config.MeterFilter.acceptNameStartsWith;
import static io.micrometer.core.instrument.config.MeterFilter.deny;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import java.util.Set;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {
  private final BuildProperties buildProperties;

  public MetricsConfig(BuildProperties buildProperties) {
    this.buildProperties = buildProperties;
  }

  @VisibleForTesting
  static final Set<String> ALLOWED_PREFIXES =
      // please keep this list alphabetized
      Set.of(
          "application",
          "cache",
          "disk",
          "executor",
          "hikaricp",
          "http",
          "jdbc",
          "jvm",
          "logback",
          "process",
          "spring",
          "system",
          "tomcat",
          "wds");

  @Bean
  MeterRegistryCustomizer<MeterRegistry> allowlistMetrics() {
    return registry -> {
      ALLOWED_PREFIXES.forEach(
          prefix -> registry.config().meterFilter(acceptNameStartsWith(prefix)));
      registry.config().meterFilter(deny()); // deny everything else
    };
  }

  @Bean
  MeterRegistryCustomizer<MeterRegistry> addWdsVersionTag() {
    return registry ->
        registry
            .config()
            .commonTags(
                new ImmutableList.Builder<Tag>()
                    .add(Tag.of("wds.version", buildProperties.getVersion()))
                    .build());
  }

  // Observation support for metrics
  @Bean
  ObservedAspect observedAspect(ObservationRegistry observationRegistry) {
    return new ObservedAspect(observationRegistry);
  }
}
