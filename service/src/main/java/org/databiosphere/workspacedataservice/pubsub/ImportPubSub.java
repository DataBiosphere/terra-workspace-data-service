package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import java.util.concurrent.CompletableFuture;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode;
import org.springframework.beans.factory.annotation.Value;

@DeploymentMode.ControlPlane
public class ImportPubSub implements PubSub {

  private final PubSubTemplate pubSubTemplate;
  private final String fullTopicName;

  public ImportPubSub(
      PubSubTemplate pubSubTemplate,
      @Value("${spring.cloud.gcp.pubsub.topic}") String topic,
      @Value("${twds.data-import.project-id}") String project) {
    this.pubSubTemplate = pubSubTemplate;
    /// projects/[project_name]/topics/[topic_name]
    this.fullTopicName = "projects/" + project + "/topics/" + topic;
  }

  // As noted in the PubSub superclass, this method encapsulates waiting for the pubsub library's
  // CompletableFuture to complete. We could also return the CompletableFuture directly, but that
  // would require callers to implement their own callbacks/waits/handlers. We could also create
  // a separate `public CompletableFuture<String> publishAsync(...)` method so callers can
  // choose between sync and async.
  public String publishSync(String message) {
    // PubSubTemplate.publish returns a future
    CompletableFuture<String> publishFuture = this.pubSubTemplate.publish(fullTopicName, message);
    // we must wait for the future to complete before returning, else we'll have spun off an
    // unwatched thread
    return publishFuture.join();
  }
}
