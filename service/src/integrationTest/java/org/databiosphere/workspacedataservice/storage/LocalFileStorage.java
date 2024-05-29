package org.databiosphere.workspacedataservice.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public class LocalFileStorage implements BackUpFileStorage {
  public LocalFileStorage() {}

  @Override
  public void streamOutputToBlobStorage(
      InputStream fromStream, String blobName, WorkspaceId workspaceId) {
    File targetFile;
    try {
      targetFile = File.createTempFile("WDS-integrationTest-LocalFileStorage-", ".sql");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (OutputStream outStream = new FileOutputStream(targetFile)) {
      try (BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(fromStream, StandardCharsets.UTF_8))) {
        int line;
        while ((line = bufferedReader.read()) != -1) {
          outStream.write(line);
        }
      }
    } catch (IOException ioEx) {
      throw new LaunchProcessException("Error streaming output during local test", ioEx);
    } finally {
      try {
        // clean up the temp file
        //noinspection ResultOfMethodCallIgnored
        targetFile.delete();
      } catch (Exception e) {
        // could not clean up the temp file; don't fail the test due to this.
      }
    }
  }

  @Override
  public void streamInputFromBlobStorage(
      OutputStream toStream, String blobName, WorkspaceId workspaceId, String authToken) {
    try (InputStream inStream =
        LocalFileStorage.class.getResourceAsStream(
            "/WDS-integrationTest-LocalFileStorage-input.sql")) {
      try (BufferedWriter bufferedWriter =
          new BufferedWriter(new OutputStreamWriter(toStream, StandardCharsets.UTF_8))) {
        int line;
        while ((line = Objects.requireNonNull(inStream).read()) != -1) {
          bufferedWriter.write((line));
        }
      }
    } catch (IOException ioEx) {
      throw new LaunchProcessException("Error streaming input during local test", ioEx);
    }
  }

  @Override
  public void deleteBlob(String blobFile, WorkspaceId workspaceId, String authToken) {
    // delete is handled in stream output for local set up
  }
}
