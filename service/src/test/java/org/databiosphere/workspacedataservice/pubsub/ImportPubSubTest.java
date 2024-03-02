package org.databiosphere.workspacedataservice.pubsub;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("control-plane")
public class ImportPubSubTest {

  @Autowired ImportPubSub importPubSub;

  @Test
  void testPubSub() {
    importPubSub.publishSync("testing");
  }
}
