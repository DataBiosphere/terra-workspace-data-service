package org.databiosphere.workspacedataservice.pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@DeploymentMode.ControlPlane
public class PubSub {

  private final TopicName topicName;

  public PubSub(
      @Value("${spring.cloud.gcp.pubsub.topic}") String topic,
      @Value("${spring.cloud.gcp.pubsub.project-id}") String project) {
    this.topicName = TopicName.of(project, topic);
  }

  public void publish(String message) throws IOException, ExecutionException, InterruptedException {
    // fully copied from
    // https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library
    Publisher publisher = null;
    try {
      // Create a publisher instance with default settings bound to the topic
      publisher = Publisher.newBuilder(topicName).build();

      ByteString data = ByteString.copyFromUtf8(message);
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

      // Once published, returns a server-assigned message id (unique within the topic)
      ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
      String messageId = messageIdFuture.get();
      System.out.println("Published message ID: " + messageId);
    } finally {
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
  }

  //  private final PubSubTemplate pubSubTemplate;
  //  private final PubSubAdmin pubSubAdmin;
  //  private final String fullTopicName;
  //
  //  public PubSub(
  //      PubSubTemplate pubSubTemplate,
  //      PubSubAdmin pubSubAdmin,
  //      @Value("${spring.cloud.gcp.pubsub.topic}") String topic,
  //      @Value("${spring.cloud.gcp.pubsub.project-id}") String project)
  //      throws IOException {
  //    this.pubSubTemplate = pubSubTemplate;
  //    this.pubSubAdmin = pubSubAdmin;
  //    /// projects/[project_name]/topics/[topic_name]
  //    this.fullTopicName = "projects/" + project + "/topics/" + topic;
  //  }
  //
  //  public void publish(String message) {
  //    this.pubSubTemplate.publish(fullTopicName, message);
  //  }
}
