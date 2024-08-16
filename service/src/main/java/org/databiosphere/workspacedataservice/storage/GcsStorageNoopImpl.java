package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * No-op implementation of GcsStorage. This implementation does nothing and returns null from any
 * method that has a return value. It exists to satisfy bean dependencies elsewhere.
 */
public class GcsStorageNoopImpl implements GcsStorage {

  @Override
  public Blob createBlob(String name) {
    return null;
  }

  @Override
  public void deleteBlob(String blobName) {}

  @Override
  public InputStream getBlobContents(String blobName) throws IOException {
    return null;
  }

  @Override
  public Iterable<Blob> getBlobsInBucket() {
    return null;
  }

  @Override
  public String getBucketName() {
    return "";
  }

  @Override
  public OutputStream getOutputStream(Blob blob) {
    return null;
  }

  @Override
  public OutputStream getOutputStream(String name) {
    return null;
  }

  @Override
  public Blob moveBlob(URI sourceUri, BlobId target) {
    return null;
  }
}
