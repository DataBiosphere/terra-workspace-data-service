package org.databiosphere.workspacedataservice.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class GcsStorageTest {
  private static final GoogleCredentials credentials;

  static {
    try {
      credentials = GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final Storage storage =
      StorageOptions.newBuilder()
          .setProjectId("broad-dsp-gcr-public") // need to figure out what this should be)
          .setCredentials(credentials)
          .build()
          .getService();
  private final String projectId = StorageOptions.getDefaultProjectId();

  public GcsStorageTest() throws IOException {}

  @Test
  public void testGetBlobSimple() {
    BlobId testBlob = BlobId.of("yuliadub-test-bucket", UUID.randomUUID().toString());

    storage.create(BlobInfo.newBuilder(testBlob).build());
    Blob blob =
        GcsStorage.getBlobFromGsPath(
            storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName());
    assertNotNull(blob);

    BlobId actualId = blob.getBlobId();
    assertEquals(testBlob.getBucket(), actualId.getBucket());
    assertEquals(testBlob.getName(), actualId.getName());
  }
}
