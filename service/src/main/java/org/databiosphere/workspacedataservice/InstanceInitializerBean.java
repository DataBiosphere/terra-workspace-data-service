package org.databiosphere.workspacedataservice;

import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedata.client.ApiException;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.distributed.DistributedLock;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.service.model.exception.CloningException;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.dao.DataAccessException;

public class InstanceInitializerBean {

  private final InstanceDao instanceDao;
  private final LeonardoDao leoDao;
  private final WorkspaceDataServiceDao wdsDao;
  private final CloneDao cloneDao;
  private final DistributedLock lock;

  private final BackupRestoreService restoreService;

  @Value("${twds.instance.workspace-id}")
  private String workspaceId;

  @Value("${twds.instance.source-workspace-id}")
  private String sourceWorkspaceId;

  @Value("${twds.startup-token}")
  private String startupToken;

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInitializerBean.class);

  /**
   * Constructor. Called by {@link InstanceInitializerConfig}.
   *
   * @see InstanceInitializerConfig
   * @param instanceDao InstanceDao
   * @param leoDao LeonardoDao
   * @param wdsDao WorkspaceDataServiceDao
   * @param cloneDao CloneDao
   * @param restoreService BackupRestoreService
   */
  public InstanceInitializerBean(
      InstanceDao instanceDao,
      LeonardoDao leoDao,
      WorkspaceDataServiceDao wdsDao,
      CloneDao cloneDao,
      BackupRestoreService restoreService,
      DistributedLock lock) {
    this.instanceDao = instanceDao;
    this.leoDao = leoDao;
    this.wdsDao = wdsDao;
    this.cloneDao = cloneDao;
    this.restoreService = restoreService;
    this.lock = lock;
  }

  /**
   * Entry point into this bean, called at WDS startup by {@link
   * InstanceInitializer#onApplicationEvent(ContextRefreshedEvent)}.
   *
   * @see InstanceInitializer
   */
  public void initializeInstance() {
    LOGGER.info("Default workspace id loaded as {}.", workspaceId);
    boolean isInCloneMode = isInCloneMode(sourceWorkspaceId);
    LOGGER.info("isInCloneMode={}.", isInCloneMode);
    if (isInCloneMode) {
      LOGGER.info("Source workspace id loaded as {}.", sourceWorkspaceId);
      boolean cloneSuccess = initCloneMode();
      if (cloneSuccess) {
        LOGGER.info("Cloning complete.");
      } else {
        initializeDefaultInstance();
      }
    } else {
      initializeDefaultInstance();
    }
  }

  /**
   * Determine if this WDS is starting as a clone of some other WDS. If a valid {@code
   * SOURCE_WORKSPACE_ID} env var is provided to this WDS, it will start in clone mode.
   *
   * @param sourceWorkspaceId value of {@code SOURCE_WORKSPACE_ID}; provided as an argument to
   *     assist with unit tests.
   * @return whether this WDS is a clone.
   */
  protected boolean isInCloneMode(String sourceWorkspaceId) {
    if (StringUtils.isNotBlank(sourceWorkspaceId)) {
      UUID sourceWorkspaceUuid;
      LOGGER.info("SourceWorkspaceId found, checking database");
      try {
        sourceWorkspaceUuid = UUID.fromString(sourceWorkspaceId);
      } catch (IllegalArgumentException e) {
        LOGGER.warn(
            "SourceWorkspaceId could not be parsed, unable to clone DB. Provided SourceWorkspaceId: {}.",
            sourceWorkspaceId);
        return false;
      }

      if (sourceWorkspaceId.equals(workspaceId)) {
        LOGGER.warn("SourceWorkspaceId and current WorkspaceId can't be the same.");
        return false;
      }

      try {
        // does the default pg schema already exist for this workspace? This could happen if
        // one replica's cloning and the other is not. We want to protect against overwriting it.
        boolean instanceAlreadyExists =
            instanceDao.instanceSchemaExists(UUID.fromString(workspaceId));
        LOGGER.info("isInCloneMode(): instanceAlreadyExists={}", instanceAlreadyExists);
        // TODO handle the case where a clone already ran, but failed; should we retry?
        return !instanceAlreadyExists;
      } catch (IllegalArgumentException e) {
        LOGGER.warn(
            "WorkspaceId could not be parsed, unable to clone DB. Provided default WorkspaceId: {}.",
            workspaceId);
        return false;
      }
    }
    LOGGER.info("No SourceWorkspaceId found, initializing default schema.");
    return false;
  }

  /*
  Cloning comes from the concept of copying an original (source) workspace data (from WDS data tables) into
  a newly created (destination) workspace. WDS at start up will always have a current WorkspaceId, which in the
  context of cloning will effectively be the destination. The SourceWorkspaceId will only be populated if the currently
  starting WDS was initiated via a clone operation and will contain the WorkspaceId of the original workspace where the cloning
  was triggered.
  */
  private boolean initCloneMode() {
    LOGGER.info("Starting in clone mode...");
    UUID trackingId = UUID.randomUUID();
    Lock sourceLock = lock.obtainLock(sourceWorkspaceId);
    try {
      // Make sure it's safe to start cloning (in case another replica is trying to clone)
      boolean lockAquired = lock.tryLock(sourceLock);
      if (!lockAquired) {
        LOGGER.info("Failed to acquire lock in initCloneMode");
        return false;
      }

      // is a clone operation already running? This can happen when WDS is running with
      // multiple replicas and another replica started first and has initiated the clone.
      // It can also happen in a corner case where this replica restarted during the clone
      // operation.
      boolean cloneAlreadyRunning = cloneDao.cloneExistsForWorkspace(sourceWorkspaceUuid);
      if (cloneAlreadyRunning) {
        LOGGER.info("Clone already running, terminating initCloneMode");
        return false;
      }

      // First, create an entry in the clone table to mark cloning has started
      LOGGER.info("Creating entry to track cloning process.");
      cloneDao.createCloneEntry(trackingId, UUID.fromString(sourceWorkspaceId));

      // Get the remote (source) WDS url from Leo, based on the source workspace id env var.
      // This call runs using the "startup token" provided by Leo.
      // Set the remote (source) WDS url on the WDS dao.
      var sourceWdsEndpoint = leoDao.getWdsEndpointUrl(startupToken);
      LOGGER.info("Retrieved source wds endpoint url {}", sourceWdsEndpoint);
      wdsDao.setWorkspaceDataServiceUrl(sourceWdsEndpoint);

      // request a backup from the remote (source) WDS
      var backupFileName = requestRemoteBackup(trackingId);

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
      lock.unlock(sourceLock);
    }
  }

  private Job<CloneResponse> currentCloneStatus(UUID trackingId) {
    var cloneStatus = cloneDao.getCloneStatus();
    if (cloneStatus == null) {
      throw new CloningException("Unexpected error: clone status was null.");
    }
    if (!trackingId.equals(cloneStatus.getJobId())) {
      throw new CloningException("Unexpected error: clone status job id did not match.");
    }
    return cloneStatus;
  }

  private String requestRemoteBackup(UUID trackingId) {
    try {
      LOGGER.info(
          "Requesting a backup file from the remote source WDS in workspace {}", sourceWorkspaceId);
      cloneDao.updateCloneEntryStatus(trackingId, CloneStatus.BACKUPQUEUED);
      var backupResponse = wdsDao.triggerBackup(startupToken, UUID.fromString(workspaceId));

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
          && wdsE.getCause() instanceof ApiException apiException
          && apiException.getCode() == 404) {
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
        "Restore from the following path on the source workspace storage container: {}",
        backupFileName);
    cloneDao.updateCloneEntryStatus(cloneJobId, CloneStatus.RESTOREQUEUED);
    var restoreResponse =
        restoreService.restoreAzureWDS("v0.2", backupFileName, cloneJobId, startupToken);
    if (!restoreResponse.getStatus().equals(JobStatus.SUCCEEDED)) {
      LOGGER.error(
          "Something went wrong with restore: {}. Starting with empty default instance schema.",
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
  private void initializeDefaultInstance() {
    Lock sourceLock = lock.obtainLock(sourceWorkspaceId);
    try {
      // Don't create schema if in the middle of cloning
      boolean lockAquired = lock.tryLock(sourceLock);
      UUID instanceId = UUID.fromString(workspaceId);
      if (!instanceDao.instanceSchemaExists(instanceId) && lockAquired) {
        instanceDao.createSchema(instanceId);
        LOGGER.info("Creating default schema id succeeded for workspaceId {}.", workspaceId);
      } else {
        LOGGER.debug(
            "Default schema for workspaceId {} already exists or is in progress; skipping creation.",
            workspaceId);
      }

    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "Workspace id could not be parsed, a default schema won't be created. Provided id: {}.",
          workspaceId);
    } catch (DataAccessException e) {
      LOGGER.error("Failed to create default schema id for workspaceId {}.", workspaceId);
    } finally {
      lock.unlock(sourceLock);
    }
  }
}
