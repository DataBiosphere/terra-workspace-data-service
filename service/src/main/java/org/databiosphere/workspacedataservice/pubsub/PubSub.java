package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PubSub {

  private final PubSubTemplate pubSubTemplate;
  private final PubSubAdmin pubSubAdmin;
  private final String fullTopicName;

  public PubSub(
      PubSubTemplate pubSubTemplate,
      PubSubAdmin pubSubAdmin,
      @Value("${spring.cloud.gcp.pubsub.topic}") String topic,
      @Value("${spring.cloud.gcp.pubsub.project-id}") String project)
      throws IOException {
    this.pubSubTemplate = pubSubTemplate;
    this.pubSubAdmin = pubSubAdmin;
    /// projects/[project_name]/topics/[topic_name]
    this.fullTopicName = "projects/" + project + "/topics/" + topic;

    //    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    //
    //    pubSubTemplate.getPublisherFactory().
    //
    //    StorageOptions storageOptions =
    //
    // StorageOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build();

  }

  public void publish(String message) {
    this.pubSubTemplate.publish(fullTopicName, message);
  }
}
