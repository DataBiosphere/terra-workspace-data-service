package org.databiosphere.workspacedataservice.metrics;

import static io.micrometer.core.instrument.config.MeterFilter.acceptNameStartsWith;
import static io.micrometer.core.instrument.config.MeterFilter.deny;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
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
                    .add(Tag.of("service", "wds"))
                    .add(Tag.of("version", buildProperties.getVersion()))
                    .build());
  }

  // central definition for the wds.import.upsertCount distribution summary
  @Bean
  RecordUpsertDistributionSummary entityUpsertCountDistributionSummary(
      MeterRegistry meterRegistry) {
    DistributionSummary distributionSummary =
        DistributionSummary.builder("wds.import.upsertCount")
            .baseUnit("upserts")
            .description("Number of upserts in this import job")
            .publishPercentiles(0.25, 0.5, 0.75, 0.95, 0.99)
            .register(meterRegistry);

    return new RecordUpsertDistributionSummary(distributionSummary);
  }

  // central definition for the wds.import.snapshotsLinked distribution summary
  @Bean
  SnapshotsLinkedDistributionSummary snapshotsLinkedDistributionSummary(
      MeterRegistry meterRegistry) {
    DistributionSummary distributionSummary =
        DistributionSummary.builder("wds.import.snapshotsLinked")
            .baseUnit("snapshots")
            .description("Number of snapshots actually linked by an import job")
            .publishPercentiles(0.25, 0.5, 0.75, 0.95, 0.99)
            .register(meterRegistry);

    return new SnapshotsLinkedDistributionSummary(distributionSummary);
  }

  // central definition for the wds.import.snapshotsConsidered distribution summary
  @Bean
  SnapshotsConsideredDistributionSummary snapshotsConsideredDistributionSummary(
      MeterRegistry meterRegistry) {
    DistributionSummary distributionSummary =
        DistributionSummary.builder("wds.import.snapshotsConsidered")
            .baseUnit("snapshots")
            .description("Number of snapshots mentioned by an import source")
            .publishPercentiles(0.25, 0.5, 0.75, 0.95, 0.99)
            .register(meterRegistry);

    return new SnapshotsConsideredDistributionSummary(distributionSummary);
  }
}
