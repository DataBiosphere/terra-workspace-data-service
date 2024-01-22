package org.databiosphere.workspacedataservice.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.boot.actuate.autoconfigure.observation.ObservationRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ObservationConfig {
  private final MeterRegistry meterRegistry;

  public ObservationConfig(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Bean
  ObservationRegistryCustomizer<ObservationRegistry> useAutowiredMeterRegistry() {
    return observationRegistry ->
        observationRegistry
            .observationConfig()
            .observationHandler(new DefaultMeterObservationHandler(meterRegistry));
  }
}
