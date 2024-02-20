package org.databiosphere.workspacedataservice.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.auth.GoogleResourceException;
import org.springframework.beans.factory.annotation.Value;

public class GcsStorage {
  private Storage storage;

  @Value("${gcs-resource-test-bucket}")
  private String bucketName;
  private GcsStorage() throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

    StorageOptions storageOptions =
        StorageOptions.newBuilder()
            .setProjectId("test") //need to figure out what this should be)
            .setCredentials(credentials)
            .build();
    this.storage = storageOptions.getService();
  }
  public static int writeBlobContents(
      Storage storage, String projectId, BlobInfo blobInfo, String contents) {
    var blob = storage.get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId));
    try (var writer = blob.writer(Storage.BlobWriteOption.userProject(projectId))) {
      return writer.write(ByteBuffer.wrap(contents.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException ex) {
      throw new GoogleResourceException(
          String.format(
              "Could not write to GCS file at %s", GcsUriUtils.getGsPathFromBlob(blobInfo)),
          ex);
    }
  }

  public static int writeBlobContents(
      Storage storage, String projectId, String gsPath, String contents) throws FileNotFoundException {
    return writeBlobContents(
        storage, projectId, getBlobFromGsPath(storage, gsPath, projectId), contents);
  }

  public static String getBlobContents(Storage storage, String projectId, BlobInfo blobInfo) {
    var blob = storage.get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId));
    var contents = blob.getContent(Blob.BlobSourceOption.userProject(projectId));
    return new String(contents, StandardCharsets.UTF_8);
  }

  public static Blob getBlobFromGsPath(Storage storage, String gspath, String targetProjectId) throws FileNotFoundException {
    BlobId locator = GcsUriUtils.parseBlobUri(gspath);

    // Provide the project of the destination of the file copy to pay if the
    // source bucket is requester pays.
    Storage.BlobGetOption[] getOptions = new Storage.BlobGetOption[0];
    if (targetProjectId != null) {
      getOptions = new Storage.BlobGetOption[] {Storage.BlobGetOption.userProject(targetProjectId)};
    }
    Blob sourceBlob = storage.get(locator, getOptions);
    if (sourceBlob == null) {
      throw new FileNotFoundException("Source file not found: '" + gspath + "'");
    }

    return sourceBlob;
  }

  public boolean deleteFileByGspath(String inGspath, String projectId) {
    if (inGspath != null) {
      BlobId blobId = GcsUriUtils.parseBlobUri(inGspath);
      return deleteWorker(blobId, projectId);
    }
    return false;
  }

  private boolean deleteWorker(BlobId blobId, String projectId) {
    Blob blob = storage.get(blobId, Storage.BlobGetOption.userProject(projectId));
    if (blob != null) {
      return blob.delete(Blob.BlobSourceOption.userProject(projectId));
    }
    //logger.warn("{} was not found and so deletion was skipped", blobId);
    return false;
  }

  /**
   * Extract the path portion (everything after the bucket name and it's trailing slash) of a gs
   * path.
   */
  private static String extractFilePathInBucket(final String path, final String bucketName) {
    return StringUtils.removeStart(path, String.format("gs://%s/", bucketName));
  }
}
