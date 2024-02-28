package org.databiosphere.workspacedataservice.pubsub;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class PubSubTest {

  @Autowired PubSub pubSub;

  @Test
  void testPubSub() {
    pubSub.publish("testing");
  }
}
