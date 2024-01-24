package org.databiosphere.workspacedataservice.jobexec;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QuartzJobTest {

  @MockBean JobDao jobDao;
  @Autowired ObservationRegistry observationRegistry;
  @Autowired MeterRegistry meterRegistry;
  private TestObservationRegistry testObservationRegistry;

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
    // set up metrics registries
    testObservationRegistry = TestObservationRegistry.create();
    Metrics.globalRegistry.add(meterRegistry);
  }

  @AfterAll
  void afterAll() {
    testObservationRegistry.clear();
    meterRegistry.clear();
    Metrics.globalRegistry.clear();
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

    // execute the TestableQuartzJob, then confirm metrics provisioned correctly
    new TestableQuartzJob(randomToken, observationRegistry).execute(mockContext);

    // metrics provisioned should be longTaskTimer and a Timer, confirm both ran
    LongTaskTimer longTaskTimer = meterRegistry.find("wds.job.execute.active").longTaskTimer();
    assertNotEquals(longTaskTimer.duration(TimeUnit.SECONDS), 0);
    Timer timer = meterRegistry.find("wds.job.execute").timer();
    assertNotEquals(timer.count(), 0);
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
