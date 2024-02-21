package org.databiosphere.workspacedataservice.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;

public class GcsStorage {
  private Storage storage;

  @Value("${twds.bucket.name}")
  private String bucketName;

  @Value("${twds.bucket.projectId}")
  private String projectId;

  public GcsStorage() throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

    StorageOptions storageOptions =
        StorageOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build();
    this.storage = storageOptions.getService();
  }

  public GcsStorage(Storage storage, String bucketName, String projectId) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.projectId = projectId;
  }

  public String getBlobContents(String blobName) {
    BlobId blobId = BlobId.of(bucketName, blobName);
    var blob = storage.get(blobId);
    var contents = blob.getContent();
    return new String(contents, StandardCharsets.UTF_8);
  }

  public BlobId createGcsFile(InputStream contents) {
    BlobId blobId = BlobId.of(bucketName, UUID.randomUUID().toString());
    storage.create(BlobInfo.newBuilder(blobId).build(), contents);
    return blobId;
  }
}
