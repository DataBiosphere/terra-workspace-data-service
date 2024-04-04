package org.databiosphere.workspacedataservice.config;

import java.time.InstantSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfig {

  /**
   * Expose {@link InstantSource#system())} as a bean to provide an override seam for tests that
   * need to verify clock behavior.
   */
  @Bean
  InstantSource instantSource() {
    return InstantSource.system();
  }
}
