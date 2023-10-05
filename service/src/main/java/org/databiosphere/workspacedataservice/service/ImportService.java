package org.databiosphere.workspacedataservice.service;

import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.impl.matchers.EverythingMatcher.allJobs;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportListener;
import org.databiosphere.workspacedataservice.dataimport.ImportQuartzJob;
import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.hashids.Hashids;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Component;

@Component
public class ImportService {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final Scheduler scheduler; // the quartz scheduler
  private final JobDao jobDao;

  private final Hashids hashids = new Hashids(UUID.randomUUID().toString(), 8);

  public ImportService(Scheduler scheduler, JobDao jobDao) {
    this.scheduler = scheduler;
    this.jobDao = jobDao;
    // register the ImportListener which provides additional logging.
    try {
      this.scheduler.getListenerManager().addJobListener(new ImportListener(), allJobs());
    } catch (SchedulerException e) {
      logger.error("error registering listener: " + e.getMessage(), e);
    }
  }

  // create a randomized job id
  private JobKey generateJobKey(String groupName) {
    String jobId = hashids.encode(ThreadLocalRandom.current().nextLong(0, Integer.MAX_VALUE));
    return new JobKey(jobId, groupName);
  }

  /**
   * Save a data-import request to the WDS db and queue up execution of that import in Quartz.
   *
   * @param importRequest the import to be saved
   * @return unique id representing the request
   */
  public ImportStatusResponse queueJob(ImportRequestServerModel importRequest) {
    JobKey jobKey = generateJobKey(importRequest.getType().getValue());
    logger.info("attempting to queue job with id {} ...", jobKey);

    // what to run
    JobDetail jobDetail =
        JobBuilder.newJob()
            .ofType(ImportQuartzJob.class)
            .withIdentity(jobKey)
            .storeDurably(false) // delete from the quartz table after the job finishes
            .withDescription("Import from " + importRequest.getUrl().toString())
            .build();

    // persist a record of this job into the WDS db
    // this automatically sets the job status to CREATED
    jobDao.createImport(jobKey.getName(), importRequest);

    // tell Quartz to run the job: run only once, start immediately
    Trigger trigger = newTrigger().forJob(jobKey).startNow().build();
    try {
      scheduler.scheduleJob(jobDetail, trigger);
    } catch (SchedulerException e) {
      logger.error("Failed to schedule import job {}: {}", jobKey, e.getMessage());
      throw new RuntimeException(e);
    }

    // with the job and its trigger successfully saved to Quartz, update the job status to QUEUED
    jobDao.updateStatus(jobKey.getName(), JobStatus.QUEUED);

    // return the jobId for this job
    return getJob(jobKey.getName());
  }

  public ImportStatusResponse getJob(String jobId) {
    try {
      return jobDao.getImport(jobId);
    } catch (EmptyResultDataAccessException notFound) {
      throw new MissingObjectException("Import job");
    }
  }
}
