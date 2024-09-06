package org.databiosphere.workspacedataservice.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class QuartzSchedulerDaoTest extends ControlPlaneTestBase {

  @MockBean Scheduler scheduler;
  @Autowired SchedulerDao schedulerDao;

  // this test needs a valid implementation of Job, so here's a noop one:
  static class FakeJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {
      // noop
    }
  }

  @Test
  void schedulesJob() throws SchedulerException {
    // create an exemplar Schedulable
    Map<String, Serializable> arguments = Map.of("first", "one", "second", 2);
    Schedulable schedulable =
        new Schedulable("my-group", "my-id", FakeJob.class, "my-description", arguments);

    // schedule our exemplar, via our dao
    schedulerDao.schedule(schedulable);

    // verify that the dao interacts properly with the underlying Quartz
    ArgumentCaptor<JobDetail> jobDetailArgument = ArgumentCaptor.forClass(JobDetail.class);
    ArgumentCaptor<Trigger> triggerArgument = ArgumentCaptor.forClass(Trigger.class);
    verify(scheduler).scheduleJob(jobDetailArgument.capture(), triggerArgument.capture());

    // verify contents of the Quartz job that was scheduled
    JobDetail actualJobDetail = jobDetailArgument.getValue();
    assertEquals("my-id", actualJobDetail.getKey().getName());
    assertEquals("my-group", actualJobDetail.getKey().getGroup());
    assertEquals(FakeJob.class, actualJobDetail.getJobClass());
    assertEquals("my-description", actualJobDetail.getDescription());
    // verify contents of that Quartz job's data map
    assertEquals(Set.of("first", "second"), actualJobDetail.getJobDataMap().keySet());
    assertEquals("one", actualJobDetail.getJobDataMap().getString("first"));
    assertEquals(2, actualJobDetail.getJobDataMap().getInt("second"));
    // verify the Quartz trigger
    Trigger actualTrigger = triggerArgument.getValue();
    assertEquals("my-id", actualTrigger.getJobKey().getName());
    assertEquals("my-group", actualTrigger.getJobKey().getGroup());

    // QuartzSchedulerDao uses a "startNow()" trigger. This means we can't inspect the exact start
    // time for the trigger, but we can assume it is very recent.
    Date actualStartTime = actualTrigger.getStartTime();
    Date now = new Date();
    long millisBeforeNow = now.getTime() - actualStartTime.getTime();
    assertThat(millisBeforeNow)
        .as("actual trigger should be within the last 2 seconds")
        .isLessThan(2000);
  }
}
