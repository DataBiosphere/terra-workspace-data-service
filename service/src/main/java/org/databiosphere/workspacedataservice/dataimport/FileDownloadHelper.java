package org.databiosphere.workspacedataservice.dataimport;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.databiosphere.workspacedataservice.service.model.exception.TdrManifestImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileDownloadHelper {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final Path tempFileDir;
  private final Multimap<String, File> fileMap;
  private final FileAttribute<Set<PosixFilePermission>> permissions =
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

  public FileDownloadHelper(String dirName) throws IOException {
    this.tempFileDir = Files.createTempDirectory(dirName, permissions);
    this.fileMap = HashMultimap.create();
  }

  public void downloadFileFromURL(String tableName, URL pathToRemoteFile) {
    try {
      Path tempFilePath =
          Files.createTempFile(
              tempFileDir, /* prefix= */ "tdr-", /* suffix= */ "download", permissions);
      logger.debug("downloading to temp file {} ...", tempFilePath);
      FileUtils.copyURLToFile(pathToRemoteFile, tempFilePath.toFile());
      // In the TDR manifest, for Azure snapshots only,
      // the first file in the list will always be a directory.
      // Attempting to import that directory
      // will fail; it has no content. To avoid those failures,
      // check files for length and ignore any that are empty
      if (tempFilePath.toFile().length() == 0) {
        logger.debug("Empty file in parquet, skipping");
        Files.delete(tempFilePath);
      } else {
        // Once the remote file has been copied to the temp file, make it read-only
        fileMap.put(tableName, tempFilePath.toFile());
      }
    } catch (IOException e) {
      throw new TdrManifestImportException(e.getMessage(), e);
    }
  }

  public void deleteFileDirectory() {
    try {
      FileUtils.deleteDirectory(tempFileDir.toFile());
    } catch (IOException e) {
      logger.error(
          "Error deleting temporary files: {} {}", e.getClass().getName(), e.getMessage(), e);
    }
  }

  public Multimap<String, File> getFileMap() {
    return fileMap;
  }
}
