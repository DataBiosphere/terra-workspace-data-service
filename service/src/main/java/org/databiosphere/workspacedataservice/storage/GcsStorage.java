package org.databiosphere.workspacedataservice.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
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

  @Value("${gcs-resource-test-bucket}")
  private String bucketName;

  @Value("${gcs-resource-projectId}")
  private String projectId;

  public GcsStorage() throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

    StorageOptions storageOptions =
    StorageOptions.newBuilder()
        .setProjectId(projectId) //need to figure out what this should be)
        .setCredentials(credentials)
        .build();
    this.storage = storageOptions.getService();
  }

  public String getBlobContents(String projectId, String path) {
    BlobId blobId = GcsUriUtils.parseBlobUri(path);
    var blob = storage.get(blobId, Storage.BlobGetOption.userProject(projectId));
    var contents = blob.getContent(Blob.BlobSourceOption.userProject(projectId));
    return new String(contents, StandardCharsets.UTF_8);
  }

  /**
   * Create a file in GCS
   *
   * @param projectId project id for billing
   * @return the Blob of the created file
   */
  public BlobId createGcsFile(String projectId, InputStream contents) {
    BlobId blobId = BlobId.of(bucketName, UUID.randomUUID().toString());
    storage.create(BlobInfo.newBuilder(blobId).build(), contents);
    return blobId;
  }

  public static Blob getBlobFromGsPath(Storage storage, String gspath) {
    BlobId locator = GcsUriUtils.parseBlobUri(gspath);
    Storage.BlobGetOption[] getOptions = new Storage.BlobGetOption[0];
    Blob sourceBlob = storage.get(locator, getOptions);

    return sourceBlob;
  }
}
