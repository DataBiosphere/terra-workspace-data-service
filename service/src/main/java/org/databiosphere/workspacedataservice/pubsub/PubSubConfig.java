package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import com.google.pubsub.v1.Subscription;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.service.PubSubService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * Creates beans when pubsub is enabled. See also NoopPubSubConfig for when pubsub is disabled.
 *
 * @see NoopPubSubConfig
 */
@Configuration
@ConditionalOnProperty(
    name = "spring.cloud.gcp.pubsub.enabled",
    havingValue = "true",
    matchIfMissing = true)
public class PubSubConfig {
  private final PubSubService pubSubService;

  public PubSubConfig(PubSubService pubSubService) {
    this.pubSubService = pubSubService;
  }

  @Bean
  PubSub getPubSub(PubSubTemplate pubSubTemplate, DataImportProperties dataImportProperties) {
    return new ImportPubSub(pubSubTemplate, dataImportProperties.getRawlsNotificationsTopic());
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
      DataImportProperties dataImportProperties) {
    Subscription subscription =
        pubSubClient.getOrCreateSubscription(
            dataImportProperties.getStatusUpdatesTopic(),
            dataImportProperties.getStatusUpdatesSubscription());

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
