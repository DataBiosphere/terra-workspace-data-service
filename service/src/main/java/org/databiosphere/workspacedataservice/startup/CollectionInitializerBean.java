package org.databiosphere.workspacedataservice.startup;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.service.model.exception.CloningException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.lang.Nullable;

public class CollectionInitializerBean {

  private final CollectionDao collectionDao;
  private final LeonardoDao leoDao;
  private final WorkspaceDataServiceDao wdsDao;
  private final CloneDao cloneDao;
  private final LockRegistry lockRegistry;

  private final BackupRestoreService restoreService;
  private final WorkspaceId workspaceId;

  @Nullable private final String sourceWorkspaceIdString;

  private final String startupToken;

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionInitializerBean.class);

  /**
   * Constructor. Called by {@link CollectionInitializerConfig}.
   *
   * @see CollectionInitializerConfig
   * @param collectionDao CollectionDao
   * @param leoDao LeonardoDao
   * @param wdsDao WorkspaceDataServiceDao
   * @param cloneDao CloneDao
   * @param restoreService BackupRestoreService
   */
  public CollectionInitializerBean(
      CollectionDao collectionDao,
      LeonardoDao leoDao,
      WorkspaceDataServiceDao wdsDao,
      CloneDao cloneDao,
      BackupRestoreService restoreService,
      LockRegistry lockRegistry,
      @SingleTenant WorkspaceId workspaceId,
      @Nullable String sourceWorkspaceIdString,
      String startupToken) {
    this.collectionDao = collectionDao;
    this.leoDao = leoDao;
    this.wdsDao = wdsDao;
    this.cloneDao = cloneDao;
    this.restoreService = restoreService;
    this.lockRegistry = lockRegistry;
    this.workspaceId = workspaceId;
    this.sourceWorkspaceIdString = sourceWorkspaceIdString;
    this.startupToken = startupToken;
  }

  /**
   * Entry point into this bean, called at WDS startup by {@link
   * CollectionInitializer#onApplicationEvent(ContextRefreshedEvent)}.
   *
   * @see CollectionInitializer
   */
  public void initializeCollection() {
    LOGGER.info("Default workspace id loaded as {}.", workspaceId);

    boolean shouldInitializeDefaultCollection =
        getCloningSourceWorkspaceId()
            .map(
                // Enter clone mode if sourceWorkspaceId is specified
                sourceWorkspaceId -> {
                  LOGGER.info(
                      "Cloning mode enabled, attempting to clone from {} into {}.",
                      sourceWorkspaceId,
                      workspaceId);
                  if (initCloneMode(sourceWorkspaceId)) {
                    LOGGER.info("Cloning complete.");
                    return false; // don't initialize default collection
                  } else {
                    LOGGER.info("Failed clone state, falling back to initialize default schema.");
                    return true; // proceed with initializing default collection
                  }
                })
            .orElse(true);

    if (shouldInitializeDefaultCollection) {
      initializeDefaultCollection();
    }
  }

  /*
  Cloning comes from the concept of copying an original (source) workspace data (from WDS data tables) into
  a newly created (destination) workspace. WDS at start up will always have a current WorkspaceId, which in the
  context of cloning will effectively be the destination. The SourceWorkspaceId will only be populated if the currently
  starting WDS was initiated via a clone operation and will contain the WorkspaceId of the original workspace where the cloning
  was triggered. This function returns false for an incomplete clone.
  */
  protected boolean initCloneMode(WorkspaceId sourceWorkspaceId) {
    LOGGER.info("Starting in clone mode...");
    UUID trackingId = UUID.randomUUID();
    Lock lock = lockRegistry.obtain(sourceWorkspaceId.toString());
    try {
      // Make sure it's safe to start cloning (in case another replica is trying to clone)
      boolean lockAcquired = lock.tryLock(1, TimeUnit.SECONDS);
      if (!lockAcquired) {
        LOGGER.info("Failed to acquire lock in initCloneMode. Exiting clone mode.");
        return true;
      }

      // Acquiring the lock means other replicas have not started or have finished cloning.
      // We can run into an existing clone operation if WDS kicks it off and has to restart,
      // or in a multi replica scenario where one is cloning and another is not.
      // If there's a clone entry and no default schema, another replica errored before completing.
      // If there's a clone entry and a default schema there's nothing for us to do here.
      if (cloneDao.cloneExistsForWorkspace(sourceWorkspaceId)) {
        boolean collectionSchemaExists =
            collectionDao.collectionSchemaExists(CollectionId.of(workspaceId.id()));
        LOGGER.info(
            "Previous clone entry found. Collection schema exists: {}.", collectionSchemaExists);
        return collectionSchemaExists;
      }

      // First, create an entry in the clone table to mark cloning has started
      LOGGER.info("Creating entry to track cloning process.");
      cloneDao.createCloneEntry(trackingId, sourceWorkspaceId);

      // Get the remote (source) WDS url from Leo, based on the source workspace id env var.
      // This call runs using the "startup token" provided by Leo.
      // Set the remote (source) WDS url on the WDS dao.
      var sourceWdsEndpoint = leoDao.getWdsEndpointUrl(startupToken);
      LOGGER.info("Retrieved source wds endpoint url {}", sourceWdsEndpoint);
      wdsDao.setWorkspaceDataServiceUrl(sourceWdsEndpoint);

      // request a backup from the remote (source) WDS
      var backupFileName = requestRemoteBackup(sourceWorkspaceId, trackingId);

      // Now that the remote backup has run, check the current clone status
      LOGGER.info("Re-checking clone job status after backup request");
      var cloneStatus = currentCloneStatus(trackingId);

      // if backup did not succeed, we cannot continue.
      if (!cloneStatus.getResult().status().equals(CloneStatus.BACKUPSUCCEEDED)
          || backupFileName == null) {
        LOGGER.error("Backup not successful, cannot restore.");
        return false;
      }

      // backup succeeded; now attempt to restore
      restoreFromRemoteBackup(backupFileName, cloneStatus.getJobId());

      // after the restore attempt, check the current clone status one more time
      // and return the result
      LOGGER.info("Re-checking clone job status after restore request");
      var finalCloneStatus = currentCloneStatus(trackingId);
      return finalCloneStatus.getStatus().equals(JobStatus.SUCCEEDED);

    } catch (Exception e) {
      LOGGER.error("An error occurred during clone mode. Error: {}", e.toString());
      // handle the interrupt if lock was interrupted
      if (e instanceof InterruptedException) {
        LOGGER.error("Error with acquiring cloning Lock: {}", e.getMessage());
        Thread.currentThread().interrupt();
      }
      try {
        cloneDao.terminateCloneToError(
            trackingId, "Backup not successful, cannot restore.", CloneTable.RESTORE);
      } catch (Exception inner) {
        LOGGER.error(
            "Furthermore, an error occurred while updating the clone job's status. Error: {}",
            inner.toString());
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  private Job<JobInput, CloneResponse> currentCloneStatus(UUID trackingId) {
    var cloneStatus = cloneDao.getCloneStatus();
    if (cloneStatus == null) {
      throw new CloningException("Unexpected error: clone status was null.");
    }
    if (!trackingId.equals(cloneStatus.getJobId())) {
      throw new CloningException("Unexpected error: clone status job id did not match.");
    }
    return cloneStatus;
  }

  private String requestRemoteBackup(WorkspaceId sourceWorkspaceId, UUID trackingId) {
    try {
      LOGGER.info(
          "Requesting a backup file from the remote source WDS in workspace {}", sourceWorkspaceId);
      cloneDao.updateCloneEntryStatus(trackingId, CloneStatus.BACKUPQUEUED);
      var backupResponse = wdsDao.triggerBackup(startupToken, workspaceId.id());

      // TODO when the wdsDao.triggerBackup is async, we will need a second call here to poll
      // for/check its status
      // note that the Job class on the next line is from the generated WDS client:
      if (backupResponse
          .getStatus()
          .equals(org.databiosphere.workspacedata.model.BackupJob.StatusEnum.SUCCEEDED)) {
        var backupFileName = backupResponse.getResult().getFilename();
        cloneDao.updateCloneEntryStatus(trackingId, CloneStatus.BACKUPSUCCEEDED);
        return backupFileName;
      } else {
        LOGGER.error("An error occurred during clone mode - backup not complete.");
        cloneDao.terminateCloneToError(
            trackingId, backupResponse.getErrorMessage(), CloneTable.BACKUP);
        return null;
      }
    } catch (WorkspaceDataServiceException wdsE) {
      if (wdsE.getCause() != null
          && wdsE.getCause() instanceof RestException restException
          && restException.getStatusCode() == HttpStatus.NOT_FOUND) {
        LOGGER.error(
            "Remote source WDS in workspace {} does not support cloning", sourceWorkspaceId);
        cloneDao.terminateCloneToError(
            trackingId,
            "The data tables in the workspace being cloned do not support cloning. "
                + "Contact the workspace owner to upgrade the version of data tables in that workspace.",
            CloneTable.BACKUP);
      } else {
        LOGGER.error(
            "An error occurred during clone mode - backup not complete: {}", wdsE.getMessage());
        cloneDao.terminateCloneToError(trackingId, wdsE.getMessage(), CloneTable.BACKUP);
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("An error occurred during clone mode - backup not complete: {}", e.getMessage());
      cloneDao.terminateCloneToError(trackingId, e.getMessage(), CloneTable.BACKUP);
      return null;
    }
  }

  /**
   * Given the path to a backup file, restore that backup file into this WDS.
   *
   * @param backupFileName backup file to restore
   * @param cloneJobId clone job to update as this restore operation proceeds
   */
  private void restoreFromRemoteBackup(String backupFileName, UUID cloneJobId) {
    LOGGER.info(
        "Restore from the following path on the destination workspace storage container: {}",
        backupFileName);
    cloneDao.updateCloneEntryStatus(cloneJobId, CloneStatus.RESTOREQUEUED);
    var restoreResponse =
        restoreService.restoreAzureWDS(
            "v0.2",
            backupFileName,
            cloneJobId,
            startupToken,
            WorkspaceId.fromString(sourceWorkspaceIdString),
            workspaceId);
    if (!restoreResponse.getStatus().equals(JobStatus.SUCCEEDED)) {
      LOGGER.error(
          "Something went wrong with restore: {}. Starting with empty default collection schema.",
          restoreResponse.getErrorMessage());
      cloneDao.terminateCloneToError(
          cloneJobId, restoreResponse.getErrorMessage(), CloneTable.RESTORE);
    } else {
      LOGGER.info("Restore Successful");
      cloneDao.updateCloneEntryStatus(cloneJobId, CloneStatus.RESTORESUCCEEDED);
    }
  }

  /*
     Create the default pg schema for this WDS. The pg schema name is the workspace id.
  */
  private void initializeDefaultCollection() {
    try {
      CollectionId collectionId = CollectionId.of(workspaceId.id());

      if (!collectionDao.collectionSchemaExists(collectionId)) {
        collectionDao.createSchema(collectionId);
        LOGGER.info("Creating default schema id succeeded for workspaceId {}.", workspaceId);
      } else {
        LOGGER.debug(
            "Default schema for workspaceId {} already exists; skipping creation.", workspaceId);
      }

    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "Workspace id {} could not be parsed, a default schema won't be created.", workspaceId);
    } catch (DataAccessException e) {
      LOGGER.error("Failed to create default schema id for workspaceId {}.", workspaceId);
    }
  }

  @VisibleForTesting
  Optional<WorkspaceId> getCloningSourceWorkspaceId() {
    if (StringUtils.isBlank(sourceWorkspaceIdString)) {
      LOGGER.info("No SourceWorkspaceId found, unable to proceed with cloning.");
      return Optional.empty();
    }
    try {
      WorkspaceId sourceId = WorkspaceId.fromString(sourceWorkspaceIdString);
      if (sourceId.equals(workspaceId)) {
        LOGGER.warn("SourceWorkspaceId and current WorkspaceId can't be the same.");
        return Optional.empty();
      }
      return Optional.of(sourceId);
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "SourceWorkspaceId {} could not be parsed, unable to clone DB.", sourceWorkspaceIdString);
      return Optional.empty();
    }
  }
}
