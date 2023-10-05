package org.databiosphere.workspacedataservice.dataimport;

import java.util.concurrent.ThreadLocalRandom;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
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

  public ImportQuartzJob(JobDao jobDao) {
    this.jobDao = jobDao;
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    // as the first step when this kicks off, set status to running
    jobDao.updateStatus(context.getJobDetail().getKey().getName(), JobStatus.RUNNING);

    // TODO: implement the actual data-import business logic! Or delegate to other
    // classes that do that.

    @SuppressWarnings("java:S2245") // RNG here is fine if it's insecure
    long sleepMillis = ThreadLocalRandom.current().nextLong(5000, 15000);
    try {
      Thread.sleep(sleepMillis);
      // the business logic for this job has completed; mark it as successful
      jobDao.updateStatus(context.getJobDetail().getKey().getName(), JobStatus.SUCCEEDED);
    } catch (InterruptedException e) {
      // ensure the thread is actually interrupted
      Thread.currentThread().interrupt();
      throw new JobExecutionException(e);
    }
  }
}
