package org.databiosphere.workspacedataservice.dataimport;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.databiosphere.workspacedataservice.service.model.exception.TdrManifestImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

public class FileDownloadHelper {

  private final DownloadHelper downloadHelper;

  public interface DownloadHelper {
    void copyURLTOFile(URL sourceUrl, File destinationFile) throws IOException;
  }

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final Path tempFileDir;
  private final Multimap<String, File> fileMap;
  private final Set<PosixFilePermission> permissions =
      EnumSet.of(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.GROUP_READ,
          PosixFilePermission.OTHERS_READ);

  public FileDownloadHelper(String dirName, DownloadHelper downloadHelper) throws IOException {
    this.tempFileDir = Files.createTempDirectory(dirName);
    this.downloadHelper = downloadHelper;
    this.fileMap = HashMultimap.create();
  }

  public FileDownloadHelper(String dirName) throws IOException {
    this(dirName, FileUtils::copyURLToFile);
  }

  @Retryable(maxAttempts = 3, backoff = @Backoff(delay = 1000))
  public void downloadFileFromURL(String tableName, URL pathToRemoteFile) {
    try {
      File tempFile =
          File.createTempFile(/* prefix= */ "tdr-", /* suffix= */ "download", tempFileDir.toFile());
      logger.info("downloading to temp file {} ...", tempFile.getPath());
      downloadHelper.copyURLTOFile(pathToRemoteFile, tempFile);
      // In the TDR manifest, for Azure snapshots only,
      // the first file in the list will always be a directory.
      // Attempting to import that directory
      // will fail; it has no content. To avoid those failures,
      // check files for length and ignore any that are empty
      if (tempFile.length() == 0) {
        logger.info("Empty file in parquet, skipping");
        Files.delete(tempFile.toPath());
      } else {
        // Once the remote file has been copied to the temp file, make it read-only
        Files.setPosixFilePermissions(tempFile.toPath(), permissions);
        fileMap.put(tableName, tempFile);
      }
    } catch (IOException e) {
      throw new TdrManifestImportException(e.getMessage(), e);
    }
  }

  public void deleteFileDirectory() {
    try {
      Files.delete(tempFileDir);
    } catch (IOException e) {
      logger.error("Error deleting temporary files: {}", e.getMessage());
    }
  }

  public Multimap<String, File> getFileMap() {
    return this.fileMap;
  }
}
