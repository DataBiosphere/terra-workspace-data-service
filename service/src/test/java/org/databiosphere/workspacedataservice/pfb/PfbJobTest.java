package org.databiosphere.workspacedataservice.pfb;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.AssertionErrors.assertEquals;

import bio.terra.datarepo.model.SnapshotModel;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.PfbQuartzJob;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
public class PfbJobTest {

  @MockBean JobDao jobDao;
  @SpyBean WorkspaceManagerDao wsmDao;

  @Test
  void doNotFailOnMissingSnapshotId() throws JobExecutionException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    when(mockContext.getMergedJobDataMap())
        .thenReturn(new JobDataMap(Map.of(ARG_TOKEN, "expectedToken")));
    // TODO is there a smarter/better way to get the correct actual path to this file
    URL resourceUrl = getClass().getResource("/test.avdl");
    when(mockContext.get(ARG_URL)).thenReturn(resourceUrl.toString());

    JobDetailImpl jobDetail = new JobDetailImpl();
    UUID jobId = UUID.randomUUID();
    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    new PfbQuartzJob(jobDao, wsmDao).execute(mockContext);

    // Should not call wsm dao
    verify(wsmDao, times(0)).createDataRepoSnapshotReference(any());
    GenericJobServerModel job = jobDao.getJob(jobId);
    assertEquals(
        "Job should be failed without snapshot id",
        GenericJobServerModel.StatusEnum.ERROR,
        job.getStatus().getValue());
  }

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    when(mockContext.getMergedJobDataMap())
        .thenReturn(new JobDataMap(Map.of(ARG_TOKEN, "expectedToken")));
    // TODO is there a smarter/better way to get the correct actual path to this file
    URL resourceUrl = getClass().getResource("/test.avdl");
    when(mockContext.get(ARG_URL)).thenReturn(resourceUrl.toString());
    JobDetailImpl jobDetail = new JobDetailImpl();
    jobDetail.setKey(new JobKey(UUID.randomUUID().toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    new PfbQuartzJob(jobDao, wsmDao).execute(mockContext);

    // This is the snapshotId given in the test pfb
    verify(wsmDao)
        .createDataRepoSnapshotReference(
            ArgumentMatchers.argThat(
                new SnapshotModelMatcher(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5"))));
  }

  private class SnapshotModelMatcher implements ArgumentMatcher<SnapshotModel> {
    private final UUID expectedSnapshotId;

    public SnapshotModelMatcher(UUID expectedSnapshotId) {
      this.expectedSnapshotId = expectedSnapshotId;
    }

    @Override
    public boolean matches(SnapshotModel exampleSnapshot) {
      return exampleSnapshot.getId().equals(expectedSnapshotId);
    }
  }
}
