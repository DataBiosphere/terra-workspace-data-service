package org.databiosphere.workspacedataservice.dao;

import static org.quartz.TriggerBuilder.newTrigger;

import org.databiosphere.workspacedataservice.dataimport.ImportQuartzJob;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository
public class QuartzSchedulerDao implements SchedulerDao {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  private final Scheduler scheduler; // the quartz scheduler

  public QuartzSchedulerDao(Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void schedule(Job<?, ?> job) {
    JobKey jobKey = new JobKey(job.getJobId().toString(), job.getJobType().name());

    // TODO: read JobData from the Job's inputs, instead of hardcoding it here
    // TODO: choose the implementing class based on the job's type, instead of hardcoding
    // ImportQuartzJob.class
    JobDetail jobDetail =
        JobBuilder.newJob()
            .ofType(ImportQuartzJob.class)
            .withIdentity(jobKey)
            .usingJobData("token", "proof-of-concept-token")
            .storeDurably(false) // delete from the quartz table after the job finishes
            .withDescription("Import for job " + job.getJobId().toString())
            .build();

    // tell Quartz to run the job: run only once, start immediately
    Trigger trigger = newTrigger().forJob(jobKey).startNow().build();
    try {
      scheduler.scheduleJob(jobDetail, trigger);
    } catch (SchedulerException e) {
      logger.error("Failed to schedule import job {}: {}", jobKey, e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
