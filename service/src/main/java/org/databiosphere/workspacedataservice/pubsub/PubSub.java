package org.databiosphere.workspacedataservice.pubsub;

import java.util.Map;

public interface PubSub {

  /**
   * Publish a GCP Pub/Sub message to a topic, wait for the message to complete publishing, and
   * return the result of the publish call.
   *
   * @param message the message to publish
   * @return result of the publish call
   */
  String publishSync(Map<String, String> message);
}
