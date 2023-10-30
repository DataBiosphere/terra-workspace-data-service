package org.databiosphere.workspacedataservice.pfb;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.ResourceList;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.PfbQuartzJob;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
// TODO: move these tests into PfbQuartzJobTest?
class PfbJobTest {

  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @Autowired RestClientRetry restClientRetry;

  @Test
  void doNotFailOnMissingSnapshotId() throws JobExecutionException, ApiException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    // This uses a non-TDR file so does not have the snapshotId
    URL resourceUrl = getClass().getResource("/minimal_data.avro");
    when(mockContext.getMergedJobDataMap())
        .thenReturn(
            new JobDataMap(Map.of(ARG_TOKEN, "expectedToken", ARG_URL, resourceUrl.toString())));

    JobDetailImpl jobDetail = new JobDetailImpl();
    UUID jobId = UUID.randomUUID();
    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    new PfbQuartzJob(jobDao, wsmDao, restClientRetry, UUID.randomUUID()).execute(mockContext);

    // Should not call wsm dao
    verify(wsmDao, times(0)).createDataRepoSnapshotReference(any(), eq(true));
    // But job should succeed
    verify(jobDao).succeeded(jobId);
  }

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException, ApiException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    URL resourceUrl = getClass().getResource("/test.avro");
    when(mockContext.getMergedJobDataMap())
        .thenReturn(
            new JobDataMap(Map.of(ARG_TOKEN, "expectedToken", ARG_URL, resourceUrl.toString())));

    JobDetailImpl jobDetail = new JobDetailImpl();
    UUID jobId = UUID.randomUUID();
    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    new PfbQuartzJob(jobDao, wsmDao, restClientRetry, UUID.randomUUID()).execute(mockContext);

    // This is the snapshotId given in the test pfb
    verify(wsmDao)
        .createDataRepoSnapshotReference(
            ArgumentMatchers.argThat(
                new SnapshotModelMatcher(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5"))),
            eq(true));
    // Job should succeed
    verify(jobDao).succeeded(jobId);
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
