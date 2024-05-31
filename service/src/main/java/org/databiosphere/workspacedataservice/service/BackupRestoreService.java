package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import com.azure.identity.extensions.jdbc.postgresql.AzurePostgresqlAuthenticationPlugin;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.dao.BackupRestoreDao;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.process.LocalProcessLauncher;
import org.databiosphere.workspacedataservice.service.model.exception.LaunchProcessException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.storage.BackUpFileStorage;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@DataPlane
public class BackupRestoreService {
  private final BackupRestoreDao<BackupResponse> backupDao;
  private final BackupRestoreDao<RestoreResponse> restoreDao;
  private final CloneDao cloneDao;
  private final BackUpFileStorage storage;
  private final CollectionDao collectionDao;
  private final NamedParameterJdbcTemplate namedTemplate;
  private final ActivityLogger activityLogger;
  private final WorkspaceId workspaceId;

  private static final Logger LOGGER = LoggerFactory.getLogger(BackupRestoreService.class);

  @Value("${twds.instance.source-workspace-id:}")
  private String sourceWorkspaceId;

  @Value("${twds.pg_dump.user:}")
  private String dbUser;

  @Value("${twds.pg_dump.dbName:}")
  private String dbName;

  @Value("${twds.pg_dump.password:}")
  private String dbPassword;

  @Value("${twds.pg_dump.port:}")
  private String dbPort;

  @Value("${twds.pg_dump.host:}")
  private String dbHost;

  @Value("${twds.pg_dump.path:}")
  private String pgDumpPath;

  @Value("${twds.pg_dump.psqlPath:}")
  private String psqlPath;

  @Value("${twds.pg_dump.useAzureIdentity:}")
  private boolean useAzureIdentity;

  public BackupRestoreService(
      BackupRestoreDao<BackupResponse> backupDao,
      BackupRestoreDao<RestoreResponse> restoreDao,
      CollectionDao collectionDao,
      BackUpFileStorage backUpFileStorage,
      CloneDao cloneDao,
      NamedParameterJdbcTemplate namedTemplate,
      ActivityLogger activityLogger,
      @SingleTenant WorkspaceId workspaceId) {
    this.backupDao = backupDao;
    this.restoreDao = restoreDao;
    this.collectionDao = collectionDao;
    this.cloneDao = cloneDao;
    this.storage = backUpFileStorage;
    this.namedTemplate = namedTemplate;
    this.activityLogger = activityLogger;
    this.workspaceId = workspaceId;
  }

  public Job<JobInput, BackupResponse> checkBackupStatus(UUID trackingId) {
    var backupJob = backupDao.getStatus(trackingId);

    if (backupJob == null) {
      throw new MissingObjectException("Backup job");
    }

    return backupJob;
  }

  public Job<JobInput, CloneResponse> checkCloneStatus() {
    return cloneDao.getCloneStatus();
  }

  public Job<JobInput, BackupResponse> backupAzureWDS(
      String version, UUID trackingId, BackupRestoreRequest backupRestoreRequest) {
    try {
      validateVersion(version);

      // if request did not specify which workspace asked for the backup, default to the current
      // workspace
      WorkspaceId requesterWorkspaceId =
          backupRestoreRequest.requestingWorkspaceId() == null
              ? workspaceId
              : WorkspaceId.of(backupRestoreRequest.requestingWorkspaceId());

      // create an entry to track progress of this backup
      backupDao.createEntry(trackingId, backupRestoreRequest);

      String blobName = generateBackupFilename();

      List<String> commandList = generateCommandList(true);
      Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

      LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
      localProcessLauncher.launchProcess(commandList, envVars);

      backupDao.updateStatus(trackingId, JobStatus.RUNNING);
      LOGGER.info("Starting streaming backup to storage.");
      storage.streamOutputToBlobStorage(
          localProcessLauncher.getInputStream(), blobName, requesterWorkspaceId);
      String error = checkForError(localProcessLauncher);

      if (StringUtils.isNotBlank(error)) {
        LOGGER.error("process error: {}", error);
        backupDao.terminateToError(trackingId, error);
      } else {
        // if no errors happen and code reaches here, the backup has been completed successfully
        backupDao.updateFilename(trackingId, blobName);
        backupDao.updateStatus(trackingId, JobStatus.SUCCEEDED);
        activityLogger.saveEventForCurrentUser(user -> user.created().backup().withId(blobName));
      }
    } catch (Exception ex) {
      LOGGER.error("Process error: {}", ex.getMessage());
      backupDao.terminateToError(trackingId, ex.getMessage());
    }

    return backupDao.getStatus(trackingId);
  }

  public Job<JobInput, RestoreResponse> restoreAzureWDS(
      String version, String backupFileName, UUID trackingId, String startupToken) {
    validateVersion(version);
    boolean doCleanup = false;
    try {
      LOGGER.info("Starting restore. ");
      logSearchPath("At beginning of restore");
      // create an entry to track progress of this restore
      restoreDao.createEntry(
          trackingId,
          new BackupRestoreRequest(
              workspaceId.id(), String.format("Restore from %s", backupFileName)));

      // generate psql query
      List<String> commandList = generateCommandList(false);
      Map<String, String> envVars = Map.of("PGPASSWORD", determinePassword());

      restoreDao.updateStatus(trackingId, JobStatus.RUNNING);
      LocalProcessLauncher localProcessLauncher = new LocalProcessLauncher();
      localProcessLauncher.launchProcess(commandList, envVars);

      LOGGER.info("Grabbing data from the backup file. ");
      // if we've reached this point, trigger cleanup of the backup file when the restore attempt is
      // done
      doCleanup = true;
      // grab blob from storage
      storage.streamInputFromBlobStorage(
          localProcessLauncher.getOutputStream(), backupFileName, workspaceId, startupToken);

      logSearchPath("Immediately after restore");

      String error = checkForError(localProcessLauncher);
      if (StringUtils.isNotBlank(error)) {
        LOGGER.error("process error: {}", error);
        restoreDao.terminateToError(trackingId, error);
        return restoreDao.getStatus(trackingId);
      }

      /* TODO: insert rows in sys_wds.collection for all schemas we just inserted.
         The following call to alterSchema only handles the default collection;
         if the source had any additional collections, they will not be handled correctly.
         If we insert rows here, we don't need the additional insert check in alterSchema.
      */

      // rename workspace schema from source to dest
      collectionDao.alterSchema(
          CollectionId.fromString(sourceWorkspaceId), CollectionId.of(workspaceId.id()));

      activityLogger.saveEventForCurrentUser(
          user -> user.restored().backup().withId(backupFileName));
      restoreDao.updateStatus(trackingId, JobStatus.SUCCEEDED);
    } catch (LaunchProcessException | PSQLException | DataAccessException ex) {
      LOGGER.error("process error: {}", ex.getMessage());
      restoreDao.terminateToError(trackingId, ex.getMessage());
    } finally {
      // reset the Postgres search path. Restoring the pg_dump set the search path to empty.
      namedTemplate.update("SET search_path TO DEFAULT;", Map.of());
      logSearchPath("After resetting search path in finally block");
      // clean up
      if (doCleanup) {
        try {
          storage.deleteBlob(backupFileName, workspaceId, startupToken);
        } catch (Exception e) {
          LOGGER.warn(
              "Error cleaning up after restore. File '{}' was not deleted from storage container: {}",
              backupFileName,
              e.getMessage(),
              e);
        }
      }
    }
    return restoreDao.getStatus(trackingId);
  }

  private void logSearchPath(String logPrefix) {
    try {
      String searchPath = namedTemplate.queryForObject("SHOW search_path;", Map.of(), String.class);
      LOGGER.info("{}, the Postgres search path is: [{}]", logPrefix, searchPath);
    } catch (Exception e) {
      // don't fail if there is a problem with logging
    }
  }

  private String determinePassword() throws PSQLException {
    if (useAzureIdentity) {
      return new String(
          new AzurePostgresqlAuthenticationPlugin(new Properties())
              .getPassword(AuthenticationRequestType.CLEARTEXT_PASSWORD));
    } else {
      return dbPassword;
    }
  }

  // Same args, different command depending on whether we're doing backup or restore.
  public List<String> generateCommandList(boolean isBackup) {
    Map<String, String> command = new LinkedHashMap<>();
    if (isBackup) {
      command.put(pgDumpPath, null);
      command.put("-b", null);
      // Grab all workspace collections/schemas in wds
      for (CollectionId id : collectionDao.listCollectionSchemas()) {
        command.put("-n", id.toString());
      }
    } else {
      command.put(psqlPath, null);
    }
    command.put("-h", dbHost);
    command.put("-p", dbPort);
    command.put("-U", dbUser);
    command.put("-d", dbName);

    List<String> commandList = new ArrayList<>();
    for (Map.Entry<String, String> entry : command.entrySet()) {
      commandList.add(entry.getKey());
      if (entry.getValue() != null) {
        commandList.add(entry.getValue());
      }
    }
    commandList.add("-v");
    commandList.add("-w");

    return commandList;
  }

  public String generateBackupFilename() {
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    String timestamp = now.format(formatter);
    return "wdsservice/cloning/backup/" + workspaceId + "-" + timestamp + ".sql";
  }

  private String checkForError(LocalProcessLauncher localProcessLauncher) {
    // materialize only the first 1024 bytes of the error stream to ensure we don't DoS ourselves
    int errorLimit = 1024;

    int exitCode = localProcessLauncher.waitForTerminate();
    if (exitCode != 0) {
      InputStream errorStream =
          localProcessLauncher.getOutputForProcess(LocalProcessLauncher.Output.ERROR);
      try {
        String error = new String(errorStream.readNBytes(errorLimit)).trim();
        LOGGER.error("process error: {}", error);
        return error;
      } catch (IOException e) {
        LOGGER.warn(
            "process failed with exit code {}, but encountered an exception reading the error output: {}",
            exitCode,
            e.getMessage());
        return "Unknown error";
      }
    }
    return "";
  }
}
