package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PubSubConfig {

  // when Pub/Sub autoconfiguration is enabled, use the real ImportPubSub bean
  @Bean
  @ConditionalOnProperty(name = "spring.cloud.gcp.pubsub.enabled", havingValue = "true")
  PubSub getPubSub(
      PubSubTemplate pubSubTemplate, @Value("${spring.cloud.gcp.pubsub.topic}") String topic) {
    return new ImportPubSub(pubSubTemplate, topic);
  }
}
