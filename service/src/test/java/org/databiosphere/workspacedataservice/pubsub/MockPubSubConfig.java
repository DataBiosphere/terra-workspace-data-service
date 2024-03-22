package org.databiosphere.workspacedataservice.pubsub;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class MockPubSubConfig {
  @Bean
  @Primary
  PubSub getMockPubSub() {
    return new MockPubSub();
  }
}
