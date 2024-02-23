package org.databiosphere.workspacedataservice.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StreamUtils;

public class GcsStorage {
  private Storage storage;

  @Value("${twds.data-import.rawls-bucket.name}")
  private String bucketName;

  // projectId in GCP (string) is similar to subscriptionId in Azure (UUID)
  @Value("${twds.data-import.rawls-bucket.projectId}")
  private String projectId;

  // Generates an instance of the storage class using the credentials the current process is running
  // under
  public GcsStorage() throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

    StorageOptions storageOptions =
        StorageOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build();
    this.storage = storageOptions.getService();
  }

  // primary here for tests, but also allows this class to be used with values other than
  // the ones provided in the config, if needed
  public GcsStorage(Storage storage, String bucketName, String projectId) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.projectId = projectId;
  }

  public String getBlobContents(String blobName) throws IOException {
    GoogleStorageResource gcsResource =
        new GoogleStorageResource(
            this.storage, String.format("gs://%s/%s", this.bucketName, blobName));
    return StreamUtils.copyToString(gcsResource.getInputStream(), Charset.defaultCharset());
  }

  public String createGcsFile(String blobName, InputStream contents) throws IOException {
    // create the GCS Resource
    GoogleStorageResource gcsResource =
        new GoogleStorageResource(
            this.storage, String.format("gs://%s/%s", this.bucketName, blobName));
    // write the data to the resource
    try (OutputStream os = gcsResource.getOutputStream()) {
      contents.transferTo(os);
    }
    return gcsResource.getBlobName();
  }
}
