package org.databiosphere.workspacedataservice.pubsub;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Creates beans when pubsub is disabled. See also PubSubConfig for when pubsub is enabled.
 *
 * @see PubSubConfig
 */
@Configuration
@ConditionalOnProperty(name = "spring.cloud.gcp.pubsub.enabled", havingValue = "false")
public class NoopPubSubConfig {

  @Bean
  PubSub getPubSub() {
    return new NoopPubSub();
  }
}
