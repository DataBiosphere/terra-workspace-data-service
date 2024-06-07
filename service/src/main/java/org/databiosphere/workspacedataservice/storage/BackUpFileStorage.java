package org.databiosphere.workspacedataservice.storage;

import java.io.InputStream;
import java.io.OutputStream;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

/* Allow for blob storage mocking. */
public interface BackUpFileStorage {
  default void streamOutputToBlobStorage(
      InputStream fromStream, String blobName, WorkspaceId workspaceId) {}

  default void streamInputFromBlobStorage(
      OutputStream toStream, String blobName, WorkspaceId workspaceId, String authToken) {}

  default void deleteBlob(String blobFile, WorkspaceId workspaceId, String authToken) {}
}
