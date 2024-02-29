package org.databiosphere.workspacedataservice.pubsub;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockPubSubConfig {

  @Bean
  @Profile("mock-pubsub")
  @Primary
  PubSub mockPubSub() {
    return new MockPubSub();
  }
}
