package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Shell/starting point for PFB import via Quartz. */
@Component
public class PfbQuartzJob extends QuartzJob {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final JobDao jobDao;

  public PfbQuartzJob(JobDao jobDao) {
    this.jobDao = jobDao;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    // TODO: implement PFB import.
    logger.info("TODO: implement PFB import.");
  }
}
