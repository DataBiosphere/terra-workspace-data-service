package org.databiosphere.workspacedataservice.pubsub;

import java.util.Map;

/**
 * No-op implementation of PubSub. This implementation does nothing and throws errors from all *
 * methods. It exists to satisfy bean dependencies elsewhere.
 */
public class NoopPubSub implements PubSub {

  @Override
  public String publishSync(Map<String, String> message) {
    throw new UnsupportedOperationException("PubSub prerequisites not configured; cannot execute.");
  }
}
