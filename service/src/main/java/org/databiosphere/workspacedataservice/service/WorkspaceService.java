package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum.SUCCEEDED;
import static org.databiosphere.workspacedataservice.service.CollectionService.NAME_DEFAULT;

import java.util.List;
import java.util.Optional;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.WorkspaceInitServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceInitJobInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkspaceService {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final JobDao jobDao;
  private final CollectionService collectionService;

  public WorkspaceService(JobDao jobDao, CollectionService collectionService) {
    this.jobDao = jobDao;
    this.collectionService = collectionService;
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

    // the default collection for any workspace has the same id as the workspace
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // idempotency: if the default collection already exists in this workspace,
    // do nothing. Return the job that previously initialized this workspace.
    if (collectionService.exists(workspaceId, collectionId)) {
      logger.debug(
          "init-workspace called for workspaceId {}, but workspace has already been initialized.",
          workspaceId);
      List<GenericJobServerModel> jobList =
          jobDao.getJobsForCollection(collectionId, Optional.of(List.of(SUCCEEDED.getValue())));
      if (!jobList.isEmpty()) {
        return jobList.get(0);
      } else {
        GenericJobServerModel fake = new GenericJobServerModel();
        fake.setJobType(GenericJobServerModel.JobTypeEnum.WORKSPACE_INIT);
        fake.setStatus(SUCCEEDED);
        fake.setErrorMessage(
            "Default collection already exists for this workspace, but could not retrieve the job that created it. This workspace is safe to use.");
        return fake;
      }
    }

    /* initialize the default collection:
       - name and description of "default"
       - collectionId equal to workspaceId
    */
    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(NAME_DEFAULT, NAME_DEFAULT);
    collectionService.save(workspaceId, collectionId, collectionRequestServerModel);

    // create a job to represent this initialization.
    WorkspaceInitJobInput workspaceInitJobInput = new WorkspaceInitJobInput(workspaceId, null);
    Job<JobInput, JobResult> job =
        Job.newJob(
            CollectionId.of(workspaceId.id()), JobType.WORKSPACE_INIT, workspaceInitJobInput);

    // persist the job to WDS's db
    GenericJobServerModel createdJob = jobDao.createJob(job);
    // immediately mark the job as successful
    jobDao.succeeded(job.getJobId());

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
}
