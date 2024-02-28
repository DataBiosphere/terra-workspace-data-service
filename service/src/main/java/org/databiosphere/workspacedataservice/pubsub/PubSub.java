package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@DeploymentMode.ControlPlane
@Component
public class PubSub {

  private final PubSubTemplate pubSubTemplate;
  private final String fullTopicName;

  public PubSub(
      PubSubTemplate pubSubTemplate,
      @Value("${spring.cloud.gcp.pubsub.topic}") String topic,
      @Value("${spring.cloud.gcp.pubsub.project-id}") String project) {
    this.pubSubTemplate = pubSubTemplate;
    /// projects/[project_name]/topics/[topic_name]
    this.fullTopicName = "projects/" + project + "/topics/" + topic;
  }

  public void publish(String message) {
    this.pubSubTemplate.publish(fullTopicName, message);
  }
}
