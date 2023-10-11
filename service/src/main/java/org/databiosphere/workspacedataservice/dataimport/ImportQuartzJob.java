package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Quartz-executable job definition for data imports.
 *
 * <p>As of this writing, the job can be queued and executed by Quartz, but the execution itself
 * will do nothing but sleep for a random time between 5 and 15 seconds
 */
@Component
public class ImportQuartzJob implements Job {

  private final JobDao jobDao;
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public ImportQuartzJob(JobDao jobDao) {
    this.jobDao = jobDao;
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    UUID jobId = UUID.fromString(context.getJobDetail().getKey().getName());

    // as the first step when this kicks off, set status to running
    jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.RUNNING);

    // retrieve the token from job data, so we can execute as the user
    String token = context.getJobDetail().getJobDataMap().getString("token");
    logger.info("executing async job with token '{}'", token);

    // TODO: implement the actual data-import business logic! Or delegate to other
    // classes that do that.

    @SuppressWarnings("java:S2245") // RNG here is fine if it's insecure
    long sleepMillis = ThreadLocalRandom.current().nextLong(5000, 15000);
    try {
      Thread.sleep(sleepMillis);
      // the business logic for this job has completed; mark it as successful
      jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.SUCCEEDED);
    } catch (InterruptedException e) {
      // ensure the thread is actually interrupted
      Thread.currentThread().interrupt();
      jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.ERROR, e.getMessage());
      throw new JobExecutionException(e);
    }
  }
}
