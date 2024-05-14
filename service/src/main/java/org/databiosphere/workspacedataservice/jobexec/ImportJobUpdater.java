package org.databiosphere.workspacedataservice.jobexec;

import java.util.List;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ImportJobUpdater {

  private final JobDao jobDao;

  public ImportJobUpdater(JobDao jobDao) {
    this.jobDao = jobDao;
  }

  @Scheduled(fixedRate = 21600000) // run every 6 hours
  public void updateImportJobs() {
    List<GenericJobServerModel> jobsToUpdate = jobDao.getOldRunningJobs();
    jobsToUpdate.stream()
        .forEach(job -> jobDao.fail(job.getJobId(), "Job failed to complete in 6 hours."));
  }
}
