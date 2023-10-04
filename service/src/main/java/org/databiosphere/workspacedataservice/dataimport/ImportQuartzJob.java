package org.databiosphere.workspacedataservice.dataimport;

import java.util.concurrent.ThreadLocalRandom;
import org.databiosphere.workspacedataservice.dao.ImportDao;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

/**
 * Quartz-executable job definition for data imports.
 *
 * <p>As of this writing, the job can be queued and executed by Quartz, but the execution itself
 * will do nothing but sleep for a random time between 1 and 15 seconds
 */
@Component
public class ImportQuartzJob implements Job {

  private final ImportDao importDao;

  public ImportQuartzJob(ImportDao importDao) {
    this.importDao = importDao;
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {

    long sleepMillis = ThreadLocalRandom.current().nextLong(1000, 15000);
    try {
      Thread.sleep(sleepMillis);
      importDao.updateStatus(context.getJobDetail().getKey().getName(), ImportStatus.SUCCEEDED);
    } catch (InterruptedException e) {
      throw new JobExecutionException(e);
    }
  }
}
