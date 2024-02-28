package org.databiosphere.workspacedataservice.sam;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class StubBearerTokenHolder {
  @Bean
  @Primary
  public BearerTokenHolder stubbedBearerTokenHolder() {
    return new BearerTokenHolder();
  }
}
