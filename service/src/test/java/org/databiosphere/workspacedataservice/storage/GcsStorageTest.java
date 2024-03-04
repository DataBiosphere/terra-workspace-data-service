package org.databiosphere.workspacedataservice.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.StreamUtils;

@Component
@SpringBootTest
@ActiveProfiles("control-plane")
@DirtiesContext
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false"
    })
class GcsStorageTest {

  @Qualifier("mockGcsStorage")
  @Autowired
  private GcsStorage storage;

  @Test
  void testCreateandGetBlobSimple() throws IOException {
    String initialString = "text";
    UUID jobId = UUID.randomUUID();
    InputStream targetStream = new ByteArrayInputStream(initialString.getBytes());
    String newBlobName = storage.createGcsFile(targetStream, jobId);
    assertThat(newBlobName).isNotNull();

    String contents =
        StreamUtils.copyToString(storage.getBlobContents(newBlobName), Charset.defaultCharset());
    assertThat(contents).isEqualTo(initialString);
  }
}
