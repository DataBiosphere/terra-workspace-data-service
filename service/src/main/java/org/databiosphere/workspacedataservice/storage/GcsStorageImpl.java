package org.databiosphere.workspacedataservice.storage;

import static java.util.Objects.requireNonNull;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseWriteChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.Optional;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.unit.DataSize;

public class GcsStorageImpl implements GcsStorage {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final Storage storage;
  private final String bucketName;
  private final DataSize blobWriterChunkSize;

  private static final DataSize MIN_CHUNK_SIZE = DataSize.ofKilobytes(256);

  /** Based on default defined in {@link BaseWriteChannel} */
  private static final DataSize DEFAULT_CHUNK_SIZE =
      DataSize.ofBytes(MIN_CHUNK_SIZE.toBytes() * 60);

  // Generates an instance of the storage class using the credentials the current process is running
  // under
  public static GcsStorageImpl create(String projectId, DataImportProperties properties)
      throws IOException {
    return new GcsStorageImpl(
        StorageOptions.newBuilder()
            // projectId in GCP (string) is similar to subscriptionId in Azure (UUID)
            .setProjectId(requireNonNull(projectId))
            .setCredentials(GoogleCredentials.getApplicationDefault())
            .build()
            .getService(),
        requireNonNull(properties.getRawlsBucketName()));
  }

  // primarily here for tests, but also allows this class to be used with values other than
  // the ones provided in the config, if needed
  GcsStorageImpl(Storage storage, String bucketName, DataSize blobWriterChunkSize) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.blobWriterChunkSize = blobWriterChunkSize;
  }

  @VisibleForTesting
  public GcsStorageImpl(Storage storage, String bucketName) {
    this(storage, bucketName, DEFAULT_CHUNK_SIZE);
  }

  @Override
  public InputStream getBlobContents(String blobName) throws IOException {
    return getGcsResource(blobName).getInputStream();
  }

  /**
   * Creates and returns an {@link OutputStream} to write to the given {@link Blob}.
   *
   * @return an {@link OutputStream} to write to the given {@link Blob}
   */
  public OutputStream getOutputStream(Blob blob) {
    return createOutputStream(blob);
  }

  /**
   * Creates a {@link Blob} with the given name in the configured bucket.
   *
   * @return an {@link OutputStream} to write to the newly created Blob
   */
  @Override
  public OutputStream getOutputStream(String name) {
    return getOutputStream(createBlob(name));
  }

  /**
   * Creates a {@link Blob} with the given name in the configured bucket.
   *
   * @return the newly created Blob
   */
  @Override
  public Blob createBlob(String name) {
    return getGcsResource(name).createBlob();
  }

  @Override
  public String getBucketName() {
    return bucketName;
  }

  @VisibleForTesting
  @Override
  public Iterable<Blob> getBlobsInBucket() {
    return storage.list(this.bucketName).getValues();
  }

  @VisibleForTesting
  @Override
  public void deleteBlob(String blobName) {
    storage.delete(this.bucketName, blobName);
  }

  /**
   * Moves a blob from one location to another. As move isn't an atomic operation, it accomplishes
   * this by copying to the new location and deleting the original. Based on: <a
   * href="https://cloud.google.com/storage/docs/samples/storage-move-file#storage_move_file-java"
   * />
   *
   * @return Blob reference to the file's new location
   */
  @Override
  public Blob moveBlob(URI sourceUri, BlobId target) {
    BlobId source = BlobId.fromGsUtilUri(sourceUri.toString());
    BlobTargetOption precondition =
        Optional.ofNullable(storage.get(target))
            .map(Blob::getGeneration)
            .map(BlobTargetOption::generationMatch)
            .orElseGet(BlobTargetOption::doesNotExist);
    Blob copiedBlob =
        storage
            .copy(
                CopyRequest.newBuilder().setSource(source).setTarget(target, precondition).build())
            .getResult();
    storage.get(source).delete();
    logger.atInfo().log("Moved %s to %s".formatted(sourceUri, copiedBlob.getBlobId()));
    return copiedBlob;
  }

  private GoogleStorageResource getGcsResource(String blobName) {
    return new GoogleStorageResource(
        this.storage, String.format("gs://%s/%s", this.bucketName, blobName));
  }

  private OutputStream createOutputStream(Blob blob) {
    WriteChannel writeChannel = blob.writer();
    writeChannel.setChunkSize((int) blobWriterChunkSize.toBytes());
    return Channels.newOutputStream(writeChannel);
  }
}
