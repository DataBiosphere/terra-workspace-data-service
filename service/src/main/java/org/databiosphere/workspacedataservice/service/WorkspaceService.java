package org.databiosphere.workspacedataservice.service;

import java.util.Optional;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
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
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkspaceService {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final JobDao jobDao;
  private final CollectionService collectionService;
  private final DataTableTypeInspector dataTableTypeInspector;
  private final WorkspaceRepository workspaceRepository;

  public WorkspaceService(
      JobDao jobDao,
      CollectionService collectionService,
      DataTableTypeInspector dataTableTypeInspector,
      WorkspaceRepository workspaceRepository) {
    this.jobDao = jobDao;
    this.collectionService = collectionService;
    this.dataTableTypeInspector = dataTableTypeInspector;
    this.workspaceRepository = workspaceRepository;
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

    // Save the workspace record to the database
    initSystemWorkspace(workspaceId);

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
    throw new RuntimeException("not implemented");
  }

  /**
   * Initialize the workspace record in the database (sys_wds.workspace) if it does not exist.
   *
   * @param workspaceId the workspace to initialize
   */
  public void initSystemWorkspace(WorkspaceId workspaceId) {
    Optional<WorkspaceRecord> maybeWorkspaceRecord = workspaceRepository.findById(workspaceId);
    if (maybeWorkspaceRecord.isEmpty()) {
      WorkspaceDataTableType dataTableType =
          dataTableTypeInspector.getWorkspaceDataTableType(workspaceId);
      WorkspaceRecord newWorkspaceRecord =
          new WorkspaceRecord(workspaceId, dataTableType, /* newFlag= */ true);
      workspaceRepository.save(newWorkspaceRecord);
    }
  }
}
