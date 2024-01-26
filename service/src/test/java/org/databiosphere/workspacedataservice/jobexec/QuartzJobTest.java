package org.databiosphere.workspacedataservice.jobexec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.observability.TestObservationRegistryConfig;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(TestObservationRegistryConfig.class)
class QuartzJobTest {

  @MockBean JobDao jobDao;
  @Autowired MeterRegistry meterRegistry;
  // overridden with a TestObservationRegistry
  @Autowired private ObservationRegistry observationRegistry;

  @BeforeAll
  void beforeAll() {
    // set up the mock jobDao to return successfully on all calls except JobDao.fail()
    GenericJobServerModel genericJobServerModel =
        new GenericJobServerModel(
            UUID.randomUUID(),
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            GenericJobServerModel.StatusEnum.QUEUED,
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    when(jobDao.createJob(any())).thenReturn(genericJobServerModel);
    when(jobDao.getJob(any())).thenReturn(genericJobServerModel);
    when(jobDao.updateStatus(any(), any())).thenReturn(genericJobServerModel);
    when(jobDao.fail(any(), any(Exception.class)))
        .thenThrow(new RuntimeException("test failed via jobDao.fail()"));
    when(jobDao.fail(any(), anyString()))
        .thenThrow(new RuntimeException("test failed via jobDao.fail()"));
  }

  /** clear all observations and metrics prior to each test */
  @BeforeEach
  void beforeEach() {
    meterRegistry.clear();
    Metrics.globalRegistry.clear();
    ((TestObservationRegistry) observationRegistry).clear();
  }

  /**
   * Testable extension of QuartzJob. This allows our tests to run something inside the QuartzJob
   * shell. Specifically, this class asserts that we can retrieve a token via TokenContextUtil from
   * within a QuartzJob's executeInternal() method.
   */
  class TestableQuartzJob extends QuartzJob {

    private final String expectedToken;

    public TestableQuartzJob(String expectedToken, ObservationRegistry registry) {
      super(registry);
      this.expectedToken = expectedToken;
    }

    @Override
    protected JobDao getJobDao() {
      return jobDao;
    }

    @Override
    protected void executeInternal(UUID jobId, JobExecutionContext context) {
      assertEquals(expectedToken, TokenContextUtil.getToken().getValue());
    }
  }

  @Test
  void tokenIsStashedAndCleaned() throws JobExecutionException {
    // set an example token, via mock, into the Quartz JobDataMap
    String expectedToken = RandomStringUtils.randomAlphanumeric(10);
    JobExecutionContext mockContext = setUpTestJob(expectedToken, UUID.randomUUID().toString());
    // assert that no token exists via TokenContextUtil
    assertTrue(
        TokenContextUtil.getToken().isEmpty(), "no token should exist before running the job");
    // execute the TestableQuartzJob, which asserts that the token passed properly from the
    // JobDataMap into job context and is therefore retrievable via TokenContextUtil
    new TestableQuartzJob(expectedToken, observationRegistry).execute(mockContext);
    // assert that the token is cleaned up and no longer reachable via TokenContextUtil
    assertTrue(
        TokenContextUtil.getToken().isEmpty(), "no token should exist after running the job");
  }

  @Test
  void correctObservation() throws JobExecutionException {
    String randomToken = RandomStringUtils.randomAlphanumeric(10);
    String jobUuid = UUID.randomUUID().toString();
    JobExecutionContext mockContext = setUpTestJob(randomToken, jobUuid);

    // this test requires a TestObservationRegistry
    TestObservationRegistry testObservationRegistry =
        assertInstanceOf(TestObservationRegistry.class, observationRegistry);

    // execute the TestableQuartzJob, then use testObservationRegistry to confirm observation
    new TestableQuartzJob(randomToken, testObservationRegistry).execute(mockContext);

    TestObservationRegistryAssert.assertThat(testObservationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasObservationWithNameEqualTo("wds.job.execute")
        .that()
        .hasHighCardinalityKeyValue("jobId", jobUuid)
        .hasLowCardinalityKeyValue("jobType", "TestableQuartzJob")
        .hasLowCardinalityKeyValue("outcome", "success")
        .hasBeenStarted()
        .hasBeenStopped();
  }

  @Test
  void correctMetrics() throws JobExecutionException {
    String randomToken = RandomStringUtils.randomAlphanumeric(10);
    JobExecutionContext mockContext = setUpTestJob(randomToken, UUID.randomUUID().toString());

    // LongTaskTimer and Timer should both be null before we run the job
    assertNull(meterRegistry.find("wds.job.execute.active").longTaskTimer());
    assertNull(meterRegistry.find("wds.job.execute").timer());

    // execute the TestableQuartzJob, then confirm metrics provisioned correctly
    new TestableQuartzJob(randomToken, observationRegistry).execute(mockContext);

    // metrics provisioned should be longTaskTimer and a Timer, confirm both ran
    LongTaskTimer longTaskTimer = meterRegistry.find("wds.job.execute.active").longTaskTimer();
    assertNotNull(longTaskTimer);
    Timer timer = meterRegistry.find("wds.job.execute").timer();
    assertNotNull(timer);

    // the LongTaskTimer for wds.job.execute.active tracks "in-flight long-running tasks".
    // since the job we just ran is complete, we expect it to show zero active tasks
    // and zero duration.
    assertEquals(0, longTaskTimer.activeTasks());
    assertEquals(0, longTaskTimer.duration(TimeUnit.SECONDS));

    // the Timer for wds.job.execute tracks history of completed jobs. We expect its duration
    // to be nonzero and its count to be 1.
    assertThat(timer.totalTime(TimeUnit.SECONDS)).isPositive();
    // with one observation, max and total should be the same
    assertEquals(timer.totalTime(TimeUnit.SECONDS), timer.max(TimeUnit.SECONDS));
    assertEquals(1, timer.count());

    // execute the TestableQuartzJob a few more times to further increment the timer count
    // and the timer total
    double previousTotal = timer.totalTime(TimeUnit.SECONDS);
    for (int i = 2; i < 10; i++) {
      new TestableQuartzJob(randomToken, observationRegistry).execute(mockContext);
      assertEquals(i, timer.count());
      // current total should be greater than previous total
      assertThat(timer.totalTime(TimeUnit.SECONDS)).isGreaterThan(previousTotal);
      previousTotal = timer.totalTime(TimeUnit.SECONDS);
    }
  }

  // sets up a job and returns the job context
  private JobExecutionContext setUpTestJob(String randomToken, String jobUuid) {
    // String jobUuid = UUID.randomUUID().toString();
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    when(mockContext.getMergedJobDataMap())
        .thenReturn(new JobDataMap(Map.of(ARG_TOKEN, randomToken)));
    JobDetailImpl jobDetail = new JobDetailImpl();
    jobDetail.setKey(new JobKey(jobUuid, "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);
    return mockContext;
  }
}
