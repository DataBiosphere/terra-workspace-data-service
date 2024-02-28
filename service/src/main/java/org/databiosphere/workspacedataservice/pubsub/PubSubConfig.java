package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.core.publisher.PubSubPublisherTemplate;
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PubSubConfig {

  @Value("${spring.cloud.gcp.pubsub.project-id}")
  private String projectId;

  @Bean
  public PubSubTemplate pubSubTemplate(
      PubSubPublisherTemplate pubSubPublisherTemplate,
      PubSubSubscriberTemplate pubSubSubscriberTemplate) {
    return new PubSubTemplate(pubSubPublisherTemplate, pubSubSubscriberTemplate);
  }
}
