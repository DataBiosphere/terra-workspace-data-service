package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
public class PfbQuartzJobTest {
  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @Autowired RestClientRetry restClientRetry;

  @Test
  void linkAllNewSnapshots() throws ApiException {
    // input is a list of 10 UUIDs
    List<UUID> input = IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).toList();
    // WSM returns no pre-existing snapshots
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // call linkSnapshots
    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    pfbQuartzJob.linkSnapshots(input);
    // capture calls
    ArgumentCaptor<SnapshotModel> argumentCaptor = ArgumentCaptor.forClass(SnapshotModel.class);
    // should have called WSM's create-snapshot-reference 10 times
    verify(wsmDao, times(input.size())).createDataRepoSnapshotReference(argumentCaptor.capture());
    // those 10 calls should have used our 10 input UUIDs
    List<SnapshotModel> actualModels = argumentCaptor.getAllValues();
    List<UUID> actualUuids = actualModels.stream().map(SnapshotModel::getId).toList();
    assertEquals(input, actualUuids);
  }

  @Test
  void linkNothingWhenAllExist() throws ApiException {
    // input is a list of 10 UUIDs
    List<UUID> input = IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).toList();

    // WSM returns all of those UUIDs as pre-existing snapshots
    ResourceList resourceList = new ResourceList();
    List<ResourceDescription> resourceDescriptions =
        input.stream().map(this::createResourceDescription).toList();
    resourceList.setResources(resourceDescriptions);
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(resourceList);

    // call linkSnapshots
    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    pfbQuartzJob.linkSnapshots(input);
    // should not call WSM's create-snapshot-reference at all
    verify(wsmDao, times(0)).createDataRepoSnapshotReference(any());
  }

  @Test
  void linkSomeWhenSomeExist() throws ApiException {
    // input is a list of 10 UUIDs
    List<UUID> input = IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).toList();

    // WSM returns some of those UUIDs as pre-existing snapshots
    ResourceList resourceList = new ResourceList();
    Random random = new Random();
    // note that the random in here can select the same UUID twice from the input, thus
    // we need the distinct()
    List<UUID> preExisting =
        IntStream.range(0, 5).mapToObj(i -> input.get(random.nextInt(input.size()))).toList();
    List<ResourceDescription> resourceDescriptions =
        preExisting.stream().map(this::createResourceDescription).distinct().toList();
    resourceList.setResources(resourceDescriptions);
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(resourceList);

    // call linkSnapshots
    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    pfbQuartzJob.linkSnapshots(input);

    // should call WSM's create-snapshot-reference only for the references that didn't already exist
    int expectedCallCount = input.size() - resourceDescriptions.size();
    ArgumentCaptor<SnapshotModel> argumentCaptor = ArgumentCaptor.forClass(SnapshotModel.class);
    verify(wsmDao, times(expectedCallCount))
        .createDataRepoSnapshotReference(argumentCaptor.capture());
    List<UUID> actual = argumentCaptor.getAllValues().stream().map(SnapshotModel::getId).toList();
    actual.forEach(
        id ->
            assertFalse(
                preExisting.contains(id),
                "should not have created reference to pre-existing snapshot"));
  }

  private ResourceDescription createResourceDescription(UUID snapshotId) {
    DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
    dataRepoSnapshotAttributes.setSnapshot(snapshotId.toString());

    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    return resourceDescription;
  }

  //
  //  @Test
  //  void doNotFailOnMissingSnapshotId() throws JobExecutionException, ApiException {
  //    JobExecutionContext mockContext = mock(JobExecutionContext.class);
  //    // This uses a non-TDR file so does not have the snapshotId
  //    URL resourceUrl = getClass().getResource("/minimal_data.avro");
  //    when(mockContext.getMergedJobDataMap())
  //        .thenReturn(
  //            new JobDataMap(Map.of(ARG_TOKEN, "expectedToken", ARG_URL,
  // resourceUrl.toString())));
  //
  //    JobDetailImpl jobDetail = new JobDetailImpl();
  //    UUID jobId = UUID.randomUUID();
  //    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
  //    when(mockContext.getJobDetail()).thenReturn(jobDetail);
  //
  //    // WSM should report no snapshots already linked to this workspace
  //    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
  //        .thenReturn(new ResourceList());
  //
  //    new PfbQuartzJob(jobDao, wsmDao, restClientRetry).execute(mockContext);
  //
  //    // Should not call wsm dao
  //    verify(wsmDao, times(0)).createDataRepoSnapshotReference(any());
  //    // But job should succeed
  //    verify(jobDao).succeeded(jobId);
  //  }
  //
  //  @Test
  //  void snapshotIdsAreParsed() throws JobExecutionException, ApiException {
  //    JobExecutionContext mockContext = mock(JobExecutionContext.class);
  //    URL resourceUrl = getClass().getResource("/test.avro");
  //    when(mockContext.getMergedJobDataMap())
  //        .thenReturn(
  //            new JobDataMap(Map.of(ARG_TOKEN, "expectedToken", ARG_URL,
  // resourceUrl.toString())));
  //
  //    JobDetailImpl jobDetail = new JobDetailImpl();
  //    UUID jobId = UUID.randomUUID();
  //    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
  //    when(mockContext.getJobDetail()).thenReturn(jobDetail);
  //
  //    // WSM should report no snapshots already linked to this workspace
  //    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
  //        .thenReturn(new ResourceList());
  //
  //    new PfbQuartzJob(jobDao, wsmDao, restClientRetry).execute(mockContext);
  //
  //    // This is the snapshotId given in the test pfb
  //    verify(wsmDao)
  //        .createDataRepoSnapshotReference(
  //            ArgumentMatchers.argThat(
  //                new PfbJobTest.SnapshotModelMatcher(
  //                    UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5"))));
  //    // Job should succeed
  //    verify(jobDao).succeeded(jobId);
  //  }
  //
  //  private class SnapshotModelMatcher implements ArgumentMatcher<SnapshotModel> {
  //    private final UUID expectedSnapshotId;
  //
  //    public SnapshotModelMatcher(UUID expectedSnapshotId) {
  //      this.expectedSnapshotId = expectedSnapshotId;
  //    }
  //
  //    @Override
  //    public boolean matches(SnapshotModel exampleSnapshot) {
  //      return exampleSnapshot.getId().equals(expectedSnapshotId);
  //    }
  //  }
}
