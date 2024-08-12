package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum.SUCCEEDED;
import static org.databiosphere.workspacedataservice.service.CollectionService.NAME_DEFAULT;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

  public GenericJobServerModel initWorkspace(
      WorkspaceId workspaceId, WorkspaceInitServerModel workspaceInitServerModel) {

    logger.info("Starting init-workspace for workspaceId {}", workspaceId);

    Optional<UUID> sourceWorkspace =
        Optional.ofNullable(workspaceInitServerModel.getClone())
            .flatMap(clone -> Optional.ofNullable(clone.getSourceWorkspaceId()));

    GenericJobServerModel job;
    if (sourceWorkspace.isPresent()) {
      // this is a clone.
      job = initClone(workspaceId, sourceWorkspace.get());
    } else {
      // this is not a clone; create an empty default collection.
      job = initEmptyWorkspace(workspaceId);
    }

    return job;
  }

  private GenericJobServerModel initEmptyWorkspace(WorkspaceId workspaceId) {

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

  private GenericJobServerModel initClone(WorkspaceId workspaceId, UUID sourceWorkspaceId) {
    // TODO AJ-1952: implement cloning
    throw new RuntimeException("not implemented");
  }
}
