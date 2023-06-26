package org.databiosphere.workspacedataservice.storage;

import java.io.InputStream;
import java.io.OutputStream;

/* Allow for blob storage mocking. */
public interface BackUpFileStorage {
    default void streamOutputToBlobStorage(InputStream fromStream, String blobName) {}
    default void streamInputFromBlobStorage(OutputStream toStream, String blobName) {}
}
