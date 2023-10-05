package org.databiosphere.workspacedataservice.dataimport;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Provides additional logging for Quartz jobs */
@Component
public class ImportListener implements JobListener {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Override
  public String getName() {
    return ImportListener.class.getSimpleName();
  }

  @Override
  public void jobToBeExecuted(JobExecutionContext context) {
    logger.info("jobToBeExecuted: {}", context.getJobDetail().getKey());
  }

  @Override
  public void jobExecutionVetoed(JobExecutionContext context) {
    logger.info("jobExecutionVetoed: {}", context.getJobDetail().getKey());
  }

  @Override
  public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
    logger.info("jobWasExecuted: {}", context.getJobDetail().getKey());
  }
}
