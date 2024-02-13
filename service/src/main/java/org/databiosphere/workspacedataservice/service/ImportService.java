package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbSchedulable;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestSchedulable;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
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
  private final CollectionService collectionService;
  private final SamDao samDao;
  private final JobDao jobDao;
  private final SchedulerDao schedulerDao;

  public ImportService(
      CollectionService collectionService,
      SamDao samDao,
      JobDao jobDao,
      SchedulerDao schedulerDao) {
    this.collectionService = collectionService;
    this.samDao = samDao;
    this.jobDao = jobDao;
    this.schedulerDao = schedulerDao;
  }

  public GenericJobServerModel createImport(
      UUID instanceUuid, ImportRequestServerModel importRequest) {
    // validate instance exists
    collectionService.validateCollection(instanceUuid);

    // validate write permission
    boolean hasWriteInstancePermission = samDao.hasWriteCollectionPermission();
    logger.debug("hasWriteInstancePermission? {}", hasWriteInstancePermission);
    if (!hasWriteInstancePermission) {
      throw new AuthorizationException("Caller does not have permission to write to instance.");
    }

    // get a token to execute the job
    String petToken = samDao.getPetToken();

    // TODO: translate the ImportRequestServerModel into a Job
    // for now, just make an example job
    logger.debug("Data import of type {} requested", importRequest.getType());

    ImportJobInput importJobInput = ImportJobInput.from(importRequest);
    Job<JobInput, JobResult> job =
        Job.newJob(CollectionId.of(instanceUuid), JobType.DATA_IMPORT, importJobInput);

    // persist the full job to WDS's db
    GenericJobServerModel createdJob = jobDao.createJob(job);
    logger.debug(
        "Job {} created for data import of type {}",
        createdJob.getJobId(),
        importRequest.getType());

    try {
      // create the arguments for the schedulable job
      Map<String, Serializable> arguments = new HashMap<>();
      arguments.put(ARG_TOKEN, petToken);
      arguments.put(ARG_URL, importRequest.getUrl().toString());
      arguments.put(ARG_COLLECTION, instanceUuid.toString());

      // create the executable job to be scheduled
      Schedulable schedulable =
          createSchedulable(importRequest.getType(), createdJob.getJobId(), arguments);

      // schedule the job. after successfully scheduling, mark the job as queued
      schedulerDao.schedule(schedulable);
      logger.debug("Job {} scheduled", createdJob.getJobId());
    } catch (Exception e) {
      // we ran into a problem scheduling the job after we inserted the row in WDS's tracking table.
      // since this job won't run, mark it as failed.
      jobDao.fail(job.getJobId(), e);
      return createdJob;
    }

    // we successfully scheduled the job; mark it as queued.
    jobDao.queued(job.getJobId());

    // return the queued job
    return createdJob;
  }

  protected Schedulable createSchedulable(
      ImportRequestServerModel.TypeEnum importType,
      UUID jobId,
      Map<String, Serializable> arguments) {
    return switch (importType) {
      case TDRMANIFEST -> new TdrManifestSchedulable(
          jobId.toString(), "TDR manifest import", arguments);
      case PFB -> new PfbSchedulable(jobId.toString(), "TODO: PFB import", arguments);
    };
  }
}
