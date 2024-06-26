package org.databiosphere.workspacedataservice.jobexec;

import java.util.List;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ImportJobUpdater {

  private final JobDao jobDao;
  public static final long UPDATE_JOB_FREQUENCY_IN_HOURS = 6;
  private static final long UPDATE_FREQUENCY_IN_MILLISECONDS =
      UPDATE_JOB_FREQUENCY_IN_HOURS * 3600 * 1000;
  private static final long MAX_INITIAL_DELAY_IN_MILLISECONDS = 3600 * 1000; // delay up to 1 hour
  private static final Logger logger = LoggerFactory.getLogger(ImportJobUpdater.class);

  public ImportJobUpdater(JobDao jobDao) {
    this.jobDao = jobDao;
  }

  @Scheduled(
      initialDelayString =
          "#{ T(java.util.concurrent.ThreadLocalRandom).current().nextInt("
              + MAX_INITIAL_DELAY_IN_MILLISECONDS
              + ") }",
      fixedRate = UPDATE_FREQUENCY_IN_MILLISECONDS)
  public void updateImportJobs() {
    List<GenericJobServerModel> jobsToUpdate = jobDao.getOldNonTerminalJobs();
    logger.info("Updating {} stalled import jobs", jobsToUpdate.size());
    jobsToUpdate.stream()
        .forEach(
            job ->
                jobDao.fail(
                    job.getJobId(),
                    "Job failed to complete in " + UPDATE_JOB_FREQUENCY_IN_HOURS + " hours."));
  }
}
