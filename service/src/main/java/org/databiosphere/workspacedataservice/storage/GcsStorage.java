package org.databiosphere.workspacedataservice.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.DataImportProperties;

public class GcsStorage {
  private final Storage storage;

  private final String bucketName;

  // projectId in GCP (string) is similar to subscriptionId in Azure (UUID)
  private final String projectId;

  // Generates an instance of the storage class using the credentials the current process is running
  // under
  public GcsStorage(DataImportProperties properties) throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    this.bucketName = properties.getRawlsBucketName();
    this.projectId = properties.getProjectId();
    StorageOptions storageOptions =
        StorageOptions.newBuilder()
            .setProjectId(this.projectId)
            .setCredentials(credentials)
            .build();
    this.storage = storageOptions.getService();
  }

  // primarily here for tests, but also allows this class to be used with values other than
  // the ones provided in the config, if needed
  public GcsStorage(Storage storage, String bucketName, String projectId) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.projectId = projectId;
  }

  public InputStream getBlobContents(String blobName) throws IOException {
    GoogleStorageResource gcsResource =
        new GoogleStorageResource(
            this.storage, String.format("gs://%s/%s", this.bucketName, blobName));
    return gcsResource.getInputStream();
  }

  public String createGcsFile(InputStream contents, UUID jobId) throws IOException {
    // create the GCS Resource
    var blobName = jobId + ".rawlsUpsert";
    GoogleStorageResource gcsResource =
        new GoogleStorageResource(
            this.storage, String.format("gs://%s/%s", this.bucketName, blobName));
    // write the data to the resource
    try (OutputStream os = gcsResource.getOutputStream()) {
      contents.transferTo(os);
    }
    return gcsResource.getBlobName();
  }

  public String getBucketName() {
    return bucketName;
  }

  @VisibleForTesting
  public Iterable<Blob> getBlobsInBucket() {
    return storage.list(this.bucketName).getValues();
  }

  @VisibleForTesting
  public void deleteBlob(String blobName) {
    storage.delete(this.bucketName, blobName);
  }
}
