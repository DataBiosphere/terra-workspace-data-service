package org.databiosphere.workspacedataservice.pubsub;

import java.util.Map;

/**
 * No-op implementation of PubSub. This implementation does nothing and returns a hardcoded empty
 * string from the one method it implements. It exists to satisfy bean dependencies elsewhere.
 */
public class NoopPubSub implements PubSub {

  @Override
  public String publishSync(Map<String, String> message) {
    return "";
  }
}
