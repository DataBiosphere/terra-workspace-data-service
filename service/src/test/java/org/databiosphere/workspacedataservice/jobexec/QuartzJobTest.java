package org.databiosphere.workspacedataservice.jobexec;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.annotations.WithTestObservationRegistry;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@WithTestObservationRegistry
class QuartzJobTest extends DataPlaneTestBase {

  @MockBean JobDao jobDao;
  @MockBean DataImportProperties dataImportProperties;
  @Autowired MeterRegistry meterRegistry;
  @Autowired TestObservationRegistry observationRegistry;

  @BeforeAll
  void beforeAll() {
    // set up the mock jobDao to return successfully on all calls except JobDao.fail()
    GenericJobServerModel genericJobServerModel =
        new GenericJobServerModel(
            UUID.randomUUID(),
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            UUID.randomUUID(),
            GenericJobServerModel.StatusEnum.QUEUED,
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    when(jobDao.createJob(any())).thenReturn(genericJobServerModel);
    when(jobDao.getJob(any())).thenReturn(genericJobServerModel);
    when(jobDao.updateStatus(any(), any())).thenReturn(genericJobServerModel);
    when(jobDao.fail(any(), anyString()))
        .thenThrow(new RuntimeException("test failed via jobDao.fail()"));

    when(dataImportProperties.isSucceedOnCompletion()).thenReturn(true);
  }

  /**
   * Testable extension of QuartzJob. This allows our tests to run something inside the QuartzJob
   * shell. Specifically, this class asserts that we can retrieve a token via TokenContextUtil from
   * within a QuartzJob's executeInternal() method.
   */
  class TestableQuartzJob extends QuartzJob {

    private final String expectedToken;
    private boolean shouldThrowError = false;

    public TestableQuartzJob(
        JobDao jobDao, ObservationRegistry observations, String expectedToken) {
      super(jobDao, observations, dataImportProperties);
      this.expectedToken = expectedToken;
    }

    public TestableQuartzJob(
        JobDao jobDao,
        String expectedToken,
        ObservationRegistry observations,
        boolean shouldThrowError) {
      this(jobDao, observations, expectedToken);
      this.shouldThrowError = shouldThrowError;
    }

    @Override
    protected void annotateObservation(Observation observation) {
      observation
          .lowCardinalityKeyValue("extraKey1", "extraValue1")
          .lowCardinalityKeyValue("extraKey2", "extraValue2");
    }

    @Override
    protected void executeInternal(UUID jobId, JobExecutionContext context) {
      assertEquals(expectedToken, TokenContextUtil.getToken().getValue());
      if (shouldThrowError) {
        throw new JobExecutionException("Forced job to fail");
      }
    }
  }

  @Test
  void tokenIsStashedAndCleaned() throws org.quartz.JobExecutionException {
    // set an example token, via mock, into the Quartz JobDataMap
    String expectedToken = RandomStringUtils.randomAlphanumeric(10);
    JobExecutionContext mockContext = setUpTestJob(expectedToken, UUID.randomUUID().toString());
    // assert that no token exists via TokenContextUtil
    assertTrue(
        TokenContextUtil.getToken().isEmpty(), "no token should exist before running the job");
    // execute the TestableQuartzJob, which asserts that the token passed properly from the
    // JobDataMap into job context and is therefore retrievable via TokenContextUtil
    getQuartzJob(expectedToken).execute(mockContext);
    // assert that the token is cleaned up and no longer reachable via TokenContextUtil
    assertTrue(
        TokenContextUtil.getToken().isEmpty(), "no token should exist after running the job");
  }

  @Test
  void correctObservation() throws org.quartz.JobExecutionException {
    String randomToken = RandomStringUtils.randomAlphanumeric(10);
    String jobUuid = UUID.randomUUID().toString();
    JobExecutionContext mockContext = setUpTestJob(randomToken, jobUuid);

    // execute the TestableQuartzJob, then use observationRegistry to confirm observation
    getQuartzJob(randomToken).execute(mockContext);

    assertThat(observationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasNumberOfObservationsWithNameEqualTo("wds.job.execute", 1)
        .hasObservationWithNameEqualTo("wds.job.execute")
        .that()
        .hasHighCardinalityKeyValue("jobId", jobUuid)
        .hasLowCardinalityKeyValue("extraKey1", "extraValue1")
        .hasLowCardinalityKeyValue("extraKey2", "extraValue2")
        .hasLowCardinalityKeyValue("outcome", StatusEnum.SUCCEEDED.getValue())
        .hasBeenStarted()
        .hasBeenStopped();
  }

  @Test
  void correctMetrics() throws org.quartz.JobExecutionException {
    String randomToken = RandomStringUtils.randomAlphanumeric(10);
    JobExecutionContext mockContext = setUpTestJob(randomToken, UUID.randomUUID().toString());

    // LongTaskTimer and Timer should both be null before we run the job
    assertNull(meterRegistry.find("wds.job.execute.active").longTaskTimer());
    assertNull(meterRegistry.find("wds.job.execute").timer());

    // execute the TestableQuartzJob, then confirm metrics provisioned correctly
    getQuartzJob(randomToken).execute(mockContext);

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
      getQuartzJob(randomToken).execute(mockContext);
      assertEquals(i, timer.count());
      // current total should be greater than previous total
      assertThat(timer.totalTime(TimeUnit.SECONDS)).isGreaterThan(previousTotal);
      previousTotal = timer.totalTime(TimeUnit.SECONDS);
    }
  }

  @Test
  void observationLogsFailure() throws org.quartz.JobExecutionException {
    String randomToken = RandomStringUtils.randomAlphanumeric(10);
    String jobUuid = UUID.randomUUID().toString();
    JobExecutionContext mockContext = setUpTestJob(randomToken, jobUuid);

    // execute the TestableQuartzJob, then confirm observation recorded failure
    getFailingQuartzJob(randomToken).execute(mockContext);

    assertThat(observationRegistry)
        .doesNotHaveAnyRemainingCurrentObservation()
        .hasNumberOfObservationsWithNameEqualTo("wds.job.execute", 1)
        .hasObservationWithNameEqualTo("wds.job.execute")
        .that()
        .hasHighCardinalityKeyValue("jobId", jobUuid)
        .hasLowCardinalityKeyValue("extraKey1", "extraValue1")
        .hasLowCardinalityKeyValue("extraKey2", "extraValue2")
        .hasLowCardinalityKeyValue("outcome", StatusEnum.ERROR.getValue())
        .hasBeenStarted()
        .hasBeenStopped()
        .hasError()
        .thenError()
        .hasMessage("Forced job to fail");
  }

  @ParameterizedTest(
      name = "Jobs should complete as success or stay running based on isSucceedOnCompletion ({0})")
  @ValueSource(booleans = {true, false})
  void jobCompletion(boolean isSucceedOnCompletion) throws org.quartz.JobExecutionException {
    // override return values from the @BeforeAll
    when(dataImportProperties.isSucceedOnCompletion()).thenReturn(isSucceedOnCompletion);

    String randomToken = RandomStringUtils.randomAlphanumeric(10);
    UUID jobUuid = UUID.randomUUID();
    JobExecutionContext mockContext = setUpTestJob(randomToken, jobUuid.toString());

    // execute the TestableQuartzJob. This will move the job to SUCCESS when
    // dataImportProperties.isSucceedOnCompletion() is true; else it will leave the job
    // in RUNNING
    getQuartzJob(randomToken).execute(mockContext);

    // assert the job is marked as succeeded ONLY when dataImportProperties.isSucceedOnCompletion()
    // is true
    var wantedNumberOfInvocations = isSucceedOnCompletion ? 1 : 0;
    verify(jobDao, times(wantedNumberOfInvocations)).succeeded(jobUuid);
  }

  // sets up a job and returns the job context
  private JobExecutionContext setUpTestJob(String randomToken, String jobUuid) {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    when(mockContext.getMergedJobDataMap())
        .thenReturn(new JobDataMap(Map.of(ARG_TOKEN, randomToken)));
    JobDetailImpl jobDetail = new JobDetailImpl();
    jobDetail.setKey(new JobKey(jobUuid, "testJobType"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);
    return mockContext;
  }

  private TestableQuartzJob getQuartzJob(String token) {
    return new TestableQuartzJob(jobDao, observationRegistry, token);
  }

  private TestableQuartzJob getFailingQuartzJob(String token) {
    return new TestableQuartzJob(jobDao, token, observationRegistry, /* shouldThrowError= */ true);
  }
}
