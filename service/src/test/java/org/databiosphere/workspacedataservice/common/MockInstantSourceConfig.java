package org.databiosphere.workspacedataservice.common;

import java.time.InstantSource;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Overrides the default {@link InstantSource} bean with a {@link MockInstantSource}. */
@TestConfiguration
public class MockInstantSourceConfig {
  @Primary
  @Bean
  MockInstantSource mockInstantSource() {
    return new MockInstantSource();
  }
}
