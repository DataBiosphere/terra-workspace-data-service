package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import com.google.pubsub.v1.Subscription;
import org.databiosphere.workspacedataservice.service.PubSubService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Configuration
@ConditionalOnProperty(name = "spring.cloud.gcp.pubsub.enabled", havingValue = "true")
public class PubSubConfig {
  private final PubSubService pubSubService;

  public PubSubConfig(PubSubService pubSubService) {
    this.pubSubService = pubSubService;
  }

  @Bean
  PubSub getPubSub(
      PubSubTemplate pubSubTemplate, @Value("${spring.cloud.gcp.pubsub.topic}") String topic) {
    return new ImportPubSub(pubSubTemplate, topic);
  }

  @Bean
  PubSubClient getPubSubClient(PubSubAdmin pubSubAdmin) {
    return new PubSubClient(pubSubAdmin);
  }

  @Bean(name = "importStatusUpdateMessageChannel")
  public MessageChannel getImportStatusUpdateMessageChannel() {
    return new DirectChannel();
  }

  @Bean
  public PubSubInboundChannelAdapter getInboundChannelAdapter(
      PubSubClient pubSubClient,
      PubSubTemplate pubSubTemplate,
      @Qualifier("importStatusUpdateMessageChannel") MessageChannel messageChannel,
      @Value("${twds.data-import.status-updates.topic}") String topicName,
      @Value("${twds.data-import.status-updates.subscription}") String subscriptionName) {
    Subscription subscription = pubSubClient.getOrCreateSubscription(topicName, subscriptionName);

    PubSubInboundChannelAdapter adapter =
        new PubSubInboundChannelAdapter(pubSubTemplate, subscription.getName());
    adapter.setOutputChannel(messageChannel);
    adapter.setAckMode(AckMode.MANUAL);
    adapter.setPayloadType(String.class);
    return adapter;
  }

  @Bean
  @ServiceActivator(inputChannel = "importStatusUpdateMessageChannel")
  public MessageHandler getImportStatusUpdateHandler() {
    return springMessage -> {
      BasicAcknowledgeablePubsubMessage originalMessage =
          springMessage
              .getHeaders()
              .get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);

      pubSubService.processPubSubMessage(originalMessage.getPubsubMessage());

      originalMessage.ack();
    };
  }
}
