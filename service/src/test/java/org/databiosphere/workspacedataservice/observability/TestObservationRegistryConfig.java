package org.databiosphere.workspacedataservice.observability;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Provides a TestObservationRegistry bean which can be used in place of Spring's default
 * ObservationRegistry. {@link TestObservationRegistry} provides conveniences for unit test
 * assertions.
 *
 * <p>To use this in your unit test, add an {@code @Import(TestObservationRegistryConfig.class)}
 * annotation to your class.
 */
@TestConfiguration
public class TestObservationRegistryConfig {
  @Bean
  ObservationRegistry testObservationRegistry() {
    return TestObservationRegistry.create();
  }
}
