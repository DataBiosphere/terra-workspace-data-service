package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * No-op implementation of GcsStorage. This implementation does nothing and throws errors from all
 * methods. It exists to satisfy bean dependencies elsewhere.
 */
public class GcsStorageNoopImpl implements GcsStorage {

  private static final String ERROR = "GcsStorage prerequisites not configured; cannot execute.";

  @Override
  public Blob createBlob(String name) {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public void deleteBlob(String blobName) {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public InputStream getBlobContents(String blobName) {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public Iterable<Blob> getBlobsInBucket() {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public String getBucketName() {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public OutputStream getOutputStream(Blob blob) {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public OutputStream getOutputStream(String name) {
    throw new UnsupportedOperationException(ERROR);
  }

  @Override
  public Blob moveBlob(URI sourceUri, BlobId target) {
    throw new UnsupportedOperationException(ERROR);
  }
}
