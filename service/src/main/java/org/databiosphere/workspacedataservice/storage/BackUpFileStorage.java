package org.databiosphere.workspacedataservice.storage;

import java.io.InputStream;

/* Allow for blob storage mocking. */
public interface BackUpFileStorage {
    default void streamOutputToBlobStorage(InputStream fromStream, String blobName) {}
    default void streamInputFromBlobStorage(String blobName) {}
}
