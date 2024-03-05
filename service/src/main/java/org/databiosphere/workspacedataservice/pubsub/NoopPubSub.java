package org.databiosphere.workspacedataservice.pubsub;

import java.util.Map;

/**
 * No-op class that replaces a real GCS-credential-enabled PubSub implementation. Use this to
 * satisfy {@link PubSub} dependencies in cases where are not running with a GCS service account.
 */
public class NoopPubSub implements PubSub {
  @Override
  public String publishSync(Map<String, String> message) {
    return null;
  }
}
