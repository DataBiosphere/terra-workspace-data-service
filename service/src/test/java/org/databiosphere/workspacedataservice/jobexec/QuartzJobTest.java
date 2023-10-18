package org.databiosphere.workspacedataservice.jobexec;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QuartzJobTest {

  @MockBean JobDao jobDao;

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
    when(jobDao.fail(any(), any()))
        .thenThrow(new RuntimeException("test failed via jobDao.fail()"));
    when(jobDao.fail(any(), any(), any()))
        .thenThrow(new RuntimeException("test failed via jobDao.fail()"));
  }

  /**
   * Testable extension of QuartzJob. This allows our tests to run something inside the QuartzJob
   * shell. Specifically, this class asserts that we can retrieve a token via TokenContextUtil from
   * within a QuartzJob's executeInternal() method.
   */
  class TestableQuartzJob extends QuartzJob {

    private final String expectedToken;

    public TestableQuartzJob(String expectedToken) {
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
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    when(mockContext.getMergedJobDataMap())
        .thenReturn(new JobDataMap(Map.of(ARG_TOKEN, expectedToken)));
    JobDetailImpl jobDetail = new JobDetailImpl();
    jobDetail.setKey(new JobKey(UUID.randomUUID().toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    // assert that no token exists via TokenContextUtil
    assertTrue(
        TokenContextUtil.getToken().isEmpty(), "no token should exist before running the job");
    // execute the TestableQuartzJob, which asserts that the token passed properly from the
    // JobDataMap into job context and is therefore retrievable via TokenContextUtil
    new TestableQuartzJob(expectedToken).execute(mockContext);
    // assert that the token is cleaned up and no longer reachable via TokenContextUtil
    assertTrue(
        TokenContextUtil.getToken().isEmpty(), "no token should exist after running the job");
  }
}
