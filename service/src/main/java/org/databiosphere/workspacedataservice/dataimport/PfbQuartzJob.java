package org.databiosphere.workspacedataservice.dataimport;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PfbQuartzJob implements Job {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    // TODO: implement PFB import.
    logger.info("TODO: implement PFB import.");
  }
}
