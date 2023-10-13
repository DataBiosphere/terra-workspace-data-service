package org.databiosphere.workspacedataservice.dao;

import static org.quartz.TriggerBuilder.newTrigger;

import org.databiosphere.workspacedataservice.shared.model.Schedulable;
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

  public void schedule(Schedulable schedulable) {
    JobKey jobKey = new JobKey(schedulable.getId(), schedulable.getGroup());

    JobDetail jobDetail =
        JobBuilder.newJob()
            .ofType(schedulable.getImplementation())
            .withIdentity(jobKey)
            .setJobData(schedulable.getArgumentsAsJobDataMap())
            .storeDurably(false) // delete from the quartz table after the job finishes
            .withDescription(schedulable.getDescription())
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
