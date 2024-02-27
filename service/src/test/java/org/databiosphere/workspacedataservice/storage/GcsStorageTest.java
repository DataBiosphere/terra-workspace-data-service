package org.databiosphere.workspacedataservice.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.springframework.util.StreamUtils;

@Component
@SpringBootTest
class GcsStorageTest {

  @Qualifier("mockGcsStorage")
  @Autowired
  private GcsStorage storage;

  @Test
  void testCreateandGetBlobSimple() throws IOException {
    String initialString = "text";
    InputStream targetStream = new ByteArrayInputStream(initialString.getBytes());
    var fileName = UUID.randomUUID().toString();
    String newBlobName = storage.createGcsFile(fileName, targetStream);

    assertEquals(fileName, newBlobName);

    String contents =
        StreamUtils.copyToString(storage.getBlobContents(newBlobName), Charset.defaultCharset());
    assertThat(contents).isEqualTo(initialString);
  }
}
