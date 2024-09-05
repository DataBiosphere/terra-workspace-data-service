package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public interface GcsStorage {
  Blob createBlob(String name);

  @VisibleForTesting
  void deleteBlob(String blobName);

  InputStream getBlobContents(String blobName) throws IOException;

  @VisibleForTesting
  Iterable<Blob> getBlobsInBucket();

  String getBucketName();

  OutputStream getOutputStream(Blob blob);

  OutputStream getOutputStream(String name);

  Blob moveBlob(URI sourceUri, BlobId target);
}
