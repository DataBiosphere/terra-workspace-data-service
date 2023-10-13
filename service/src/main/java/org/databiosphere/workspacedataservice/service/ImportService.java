package org.databiosphere.workspacedataservice.service;

import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ImportService {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final InstanceService instanceService;
  private final SamDao samDao;
  private final JobDao jobDao;
  private final SchedulerDao schedulerDao;
  private final ActivityLogger activityLogger;

  public ImportService(
      InstanceService instanceService,
      SamDao samDao,
      JobDao jobDao,
      SchedulerDao schedulerDao,
      ActivityLogger activityLogger) {
    this.instanceService = instanceService;
    this.samDao = samDao;
    this.jobDao = jobDao;
    this.schedulerDao = schedulerDao;
    this.activityLogger = activityLogger;
  }

  public GenericJobServerModel createImport(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    // validate instance exists
    instanceService.validateInstance(instanceUuid);

    // validate write permission
    boolean hasWriteInstancePermission = samDao.hasWriteInstancePermission();
    logger.debug("hasWriteInstancePermission? {}", hasWriteInstancePermission);
    if (!hasWriteInstancePermission) {
      throw new AuthorizationException("Caller does not have permission to write to instance.");
    }

    // TODO: translate the ImportRequestServerModel into a Job
    // for now, just make an example job
    Job<JobInput, JobResult> job = Job.newJob(JobType.DATA_IMPORT, JobInput.empty());

    // persist the job
    GenericJobServerModel createdJob = jobDao.createJob(job);

    // schedule the job. after successfully scheduling, mark the job as queued
    schedulerDao.schedule(job);
    jobDao.updateStatus(job.getJobId(), GenericJobServerModel.StatusEnum.QUEUED);

    // TODO: activity log? Or is that part of the job itself?

    // return the queued job
    return createdJob;
  }
}
