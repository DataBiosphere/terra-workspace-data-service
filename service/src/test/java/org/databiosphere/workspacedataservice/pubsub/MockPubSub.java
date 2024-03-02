package org.databiosphere.workspacedataservice.pubsub;

public class MockPubSub implements PubSub {

  public String publishSync(String message) {
    return this.getClass().getName();
  }
}
