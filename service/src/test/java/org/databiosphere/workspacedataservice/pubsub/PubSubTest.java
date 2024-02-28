package org.databiosphere.workspacedataservice.pubsub;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class PubSubTest {

  @Autowired PubSub pubSub;

  @Test
  void testPubSub() throws IOException, ExecutionException, InterruptedException {
    pubSub.publish("testing");
  }
}
