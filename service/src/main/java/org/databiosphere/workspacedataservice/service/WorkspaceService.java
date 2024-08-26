package org.databiosphere.workspacedataservice.service;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CloneDao;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.DefaultCollectionCreationResult;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceInitJobInput;
import org.databiosphere.workspacedataservice.workspace.WorkspaceInitJobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkspaceService {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final JobDao jobDao;
  private final CollectionService collectionService;
  private final DataTableTypeInspector dataTableTypeInspector;
  private final CloneDao cloneDao;
  private final CollectionDao collectionDao;
  private final ControlPlaneBackupRestoreService backupRestoreService;

  public WorkspaceService(
      JobDao jobDao,
      CollectionService collectionService,
      DataTableTypeInspector dataTableTypeInspector,
      CloneDao cloneDao,
      CollectionDao collectionDao,
      ControlPlaneBackupRestoreService backupRestoreService) {
    this.jobDao = jobDao;
    this.collectionService = collectionService;
    this.dataTableTypeInspector = dataTableTypeInspector;
    this.cloneDao = cloneDao;
    this.collectionDao = collectionDao;
    this.backupRestoreService = backupRestoreService;
  }

  public WorkspaceDataTableType getDataTableType(WorkspaceId workspaceId) {
    return dataTableTypeInspector.getWorkspaceDataTableType(workspaceId);
  }

  /**
   * Initialize WDS for a given workspace. As of this writing, initialization will create an empty
   * default collection for new workspaces, and clone collections for cloned workspaces.
   *
   * <p>This implementation is synchronous. However, it returns a reference to an async job. We may
   * need to move to an async implementation, so we're using the async payload to allow future
   * change.
   *
   * @param workspaceId the workspace to initialize
   * @param workspaceInitServerModel initialization arguments, including information about clone
   *     source
   * @return reference to the initialization job
   */
  public GenericJobServerModel initWorkspace(
      WorkspaceId workspaceId, WorkspaceInitServerModel workspaceInitServerModel) {

    logger.info("Starting init-workspace for workspaceId {}", workspaceId);

    // translate the input arguments to JobInput format
    WorkspaceInitJobInput jobInput =
        WorkspaceInitJobInput.from(workspaceId, workspaceInitServerModel);

    // branch for clones vs. non-clones
    if (jobInput.sourceWorkspaceId() != null) {
      return initClone(jobInput);
    } else {
      return initEmptyWorkspace(jobInput);
    }
  }

  /**
   * Initialization steps for non-clones. Creates an empty default collection.
   *
   * @param jobInput initialization arguments
   * @return reference to the initialization job
   */
  private GenericJobServerModel initEmptyWorkspace(WorkspaceInitJobInput jobInput) {

    WorkspaceId workspaceId = jobInput.workspaceId();

    // ask collectionService to create the default collection; this call is idempotent
    DefaultCollectionCreationResult defaultCollectionCreationResult =
        collectionService.createDefaultCollection(workspaceId);

    // create a job to represent this initialization.
    WorkspaceInitJobInput workspaceInitJobInput = new WorkspaceInitJobInput(workspaceId, null);
    Job<JobInput, JobResult> job =
        Job.newJob(
            CollectionId.of(workspaceId.id()), JobType.WORKSPACE_INIT, workspaceInitJobInput);

    // persist the job to WDS's db
    GenericJobServerModel createdJob = jobDao.createJob(job);
    // immediately mark the job as successful
    jobDao.succeeded(job.getJobId());

    // add the job result to the response
    /* TODO AJ-1401: this job result is not persisted to the database. If a user looks up this job later, we won't
        return the result. We need to implement AJ-1401 before we can actually persist and job results. For now,
        we return the result in the call to the init-workspace API only.
    */

    WorkspaceInitJobResult jobResult =
        new WorkspaceInitJobResult(
            /* defaultCollectionCreated= */ defaultCollectionCreationResult.created(),
            /* isClone= */ false);

    createdJob.setResult(jobResult);

    // return the successful job
    return createdJob;
  }

  /**
   * Initialization steps for clones. TODO AJ-1952 add doc explaining the implementation
   *
   * @param jobInput initialization arguments
   * @return reference to the initialization job
   */
  private GenericJobServerModel initClone(WorkspaceInitJobInput jobInput) {
    // TODO AJ-1952: implement cloning
    /* Copying from CollectionInitializerBean.initCloneMode as a first draft
     */
    logger.info("Initializing clone...");
    WorkspaceId workspaceId = jobInput.workspaceId();
    // TODO is this workspace_init or sync_clone?
    Job<JobInput, JobResult> job =
        Job.newJob(CollectionId.of(workspaceId.id()), JobType.WORKSPACE_INIT, jobInput);
    // persist the job to WDS's db
    GenericJobServerModel createdJob = jobDao.createJob(job);
    UUID trackingId = UUID.randomUUID();

    // TODO: Are replicas an issue here?  do we need to lock?
    // We shouldn't be here unless this exists
    WorkspaceId sourceWorkspaceId = jobInput.sourceWorkspaceId();
    if (cloneDao.cloneExistsForWorkspace(sourceWorkspaceId)) {
      // TODO can I do this with CollectionService so we don't have both service and dao?
      boolean collectionSchemaExists =
          collectionDao.collectionSchemaExists(CollectionId.of(workspaceId.id()));
      logger.info(
          "Previous clone entry found. Collection schema exists: {}.", collectionSchemaExists);
      // TODO what is the appropriate behavior here?
      createdJob.setResult(new DefaultCollectionCreationResult(false, null));
      createdJob.setErrorMessage("Collection schema already exists");
      return createdJob;
    }

    // First, create an entry in the clone table to mark cloning has started
    logger.info("Creating entry to track cloning process.");
    cloneDao.createCloneEntry(trackingId, sourceWorkspaceId);

    // TODO implement SQL-based copy of schemas
    org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest body =
        new org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest(
            workspaceId.id(), "clone requested");
    // TODO new version?
    Job<JobInput, BackupResponse> response =
        backupRestoreService.backupAzureWDS("v0.2", trackingId, body, sourceWorkspaceId);
    // TODO check on clone status
    String filename = response.getResult().filename();
    backupRestoreService.restoreAzureWDS(
        "v0.2", filename, trackingId, "oh no i don't have a token", sourceWorkspaceId, workspaceId);

    // after the restore attempt, check the current clone status one more time
    // and return the result
    // TODO is this necessary here?
    logger.info("Re-checking clone job status after restore request");
    var finalCloneStatus = cloneDao.getCloneStatus();
    //      return finalCloneStatus.getStatus().equals(JobStatus.SUCCEEDED);

    //    } catch (Exception e) {
    //      LOGGER.error("An error occurred during clone mode. Error: {}", e.toString());
    //      // handle the interrupt if lock was interrupted
    //      if (e instanceof InterruptedException) {
    //        LOGGER.error("Error with acquiring cloning Lock: {}", e.getMessage());
    //        Thread.currentThread().interrupt();
    //      }
    //      try {
    //        cloneDao.terminateCloneToError(
    //            trackingId, "Backup not successful, cannot restore.", CloneTable.RESTORE);
    //      } catch (Exception inner) {
    //        LOGGER.error(
    //            "Furthermore, an error occurred while updating the clone job's status. Error: {}",
    //            inner.toString());
    //      }
    //      return false;
    //    } finally {
    //      lock.unlock();
    //    }
    //  }

    // mark the job as successful
    jobDao.succeeded(job.getJobId());
    // ask collectionService to create the default collection; this call is idempotent
    DefaultCollectionCreationResult defaultCollectionCreationResult =
        collectionService.createDefaultCollection(jobInput.workspaceId());
    WorkspaceInitJobResult jobResult =
        new WorkspaceInitJobResult(
            /* defaultCollectionCreated= */ defaultCollectionCreationResult.created(),
            /* isClone= */ true);

    createdJob.setResult(jobResult);

    // return the successful job
    return createdJob;
    //    throw new RuntimeException("not implemented");
  }
}
