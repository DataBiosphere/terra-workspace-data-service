package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.ExpirationPolicy;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubClient.class);
  private final PubSubAdmin admin;

  public PubSubClient(PubSubAdmin admin) {
    this.admin = admin;
  }

  public Subscription getOrCreateSubscription(String topicName, String subscriptionName) {
    // In live environments, the topic for import status updates should already exist,
    // since it has been Terraformed. In BEEs, either CWDS or Import Service (whichever
    // starts first) needs to create a topic for the BEE.
    LOGGER.info(
        "Subscribing to PubSub topic {} using subscription {}", topicName, subscriptionName);
    Topic topic = admin.getTopic(topicName);
    if (topic == null) {
      LOGGER.info("Topic {} not found, creating...", topicName);
      topic = admin.createTopic(topicName);
    }

    // Get or create a pull subscription for the import status updates topic.
    Subscription subscription = admin.getSubscription(subscriptionName);
    if (subscription == null) {
      LOGGER.info("Subscription {} not found, creating...", subscriptionName);
      subscription =
          admin.createSubscription(
              Subscription.newBuilder()
                  .setName(subscriptionName)
                  .setTopic(topic.getName())
                  .setExpirationPolicy(ExpirationPolicy.getDefaultInstance())
                  .setMessageRetentionDuration(Duration.newBuilder().setSeconds(86_400)));
    }

    return subscription;
  }
}
