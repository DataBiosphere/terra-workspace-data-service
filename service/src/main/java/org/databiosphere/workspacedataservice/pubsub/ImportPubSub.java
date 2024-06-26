package org.databiosphere.workspacedataservice.pubsub;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ImportPubSub implements PubSub {

  private final PubSubTemplate pubSubTemplate;
  private final String topic;

  public ImportPubSub(PubSubTemplate pubSubTemplate, String topic) {
    this.pubSubTemplate = pubSubTemplate;
    this.topic = topic;
  }

  // As noted in the PubSub superclass, this method encapsulates waiting for the pubsub library's
  // CompletableFuture to complete. We could also return the CompletableFuture directly, but that
  // would require callers to implement their own callbacks/waits/handlers. We could also create
  // a separate `public CompletableFuture<String> publishAsync(...)` method so callers can
  // choose between sync and async.
  public String publishSync(Map<String, String> message) {
    // PubSubTemplate.publish returns a future
    // Rawls expects the actual data to be in the headers rather than the payload
    CompletableFuture<String> publishFuture = pubSubTemplate.publish(topic, "", message);
    // we must wait for the future to complete before returning, else we'll have spun off an
    // unwatched thread
    return publishFuture.join();
  }
}
