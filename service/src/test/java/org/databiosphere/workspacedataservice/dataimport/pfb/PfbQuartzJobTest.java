package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.buildQuartzJob;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.stubJobContext;
import static org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class PfbQuartzJobTest {
  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @MockBean BatchWriteService batchWriteService;
  @MockBean ActivityLogger activityLogger;
  @Autowired RestClientRetry restClientRetry;

  // test resources used below
  @Value("classpath:avro/minimal_data.avro")
  Resource minimalDataAvroResource;

  @Value("classpath:avro/test.avro")
  Resource testAvroResource;

  private static final String INSTANCE = "aaaabbbb-cccc-dddd-1111-222233334444";

  @Test
  void linkAllNewSnapshots() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());
    // WSM returns no pre-existing snapshots
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    // call linkSnapshots
    PfbQuartzJob pfbQuartzJob =
        buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger);
    pfbQuartzJob.linkSnapshots(input);
    // capture calls
    ArgumentCaptor<SnapshotModel> argumentCaptor = ArgumentCaptor.forClass(SnapshotModel.class);
    // should have called WSM's create-snapshot-reference 10 times
    verify(wsmDao, times(input.size())).linkSnapshotForPolicy(argumentCaptor.capture());
    // those 10 calls should have used our 10 input UUIDs
    List<SnapshotModel> actualModels = argumentCaptor.getAllValues();
    Set<UUID> actualUuids =
        actualModels.stream().map(SnapshotModel::getId).collect(Collectors.toSet());
    assertEquals(input, actualUuids);
  }

  @Test
  void linkNothingWhenAllExist() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());

    // WSM returns all of those UUIDs as pre-existing snapshots
    ResourceList resourceList = new ResourceList();
    List<ResourceDescription> resourceDescriptions =
        input.stream().map(this::createResourceDescription).toList();
    resourceList.setResources(resourceDescriptions);
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(resourceList);

    // call linkSnapshots
    PfbQuartzJob pfbQuartzJob =
        buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger);
    pfbQuartzJob.linkSnapshots(input);
    // should not call WSM's create-snapshot-reference at all
    verify(wsmDao, times(0)).linkSnapshotForPolicy(any());
  }

  @Test
  void linkSomeWhenSomeExist() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());

    // WSM returns some of those UUIDs as pre-existing snapshots
    ResourceList resourceList = new ResourceList();
    Random random = new Random();
    // note that the random in here can select the same UUID twice from the input, thus
    // we need the distinct()
    List<UUID> preExisting =
        IntStream.range(0, 5)
            .mapToObj(i -> input.stream().toList().get(random.nextInt(input.size())))
            .toList();
    List<ResourceDescription> resourceDescriptions =
        preExisting.stream().map(this::createResourceDescription).distinct().toList();
    resourceList.setResources(resourceDescriptions);
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(resourceList);

    // call linkSnapshots
    PfbQuartzJob pfbQuartzJob =
        buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger);
    pfbQuartzJob.linkSnapshots(input);

    // should call WSM's create-snapshot-reference only for the references that didn't already exist
    int expectedCallCount = input.size() - resourceDescriptions.size();
    ArgumentCaptor<SnapshotModel> argumentCaptor = ArgumentCaptor.forClass(SnapshotModel.class);
    verify(wsmDao, times(expectedCallCount)).linkSnapshotForPolicy(argumentCaptor.capture());
    List<UUID> actual = argumentCaptor.getAllValues().stream().map(SnapshotModel::getId).toList();
    actual.forEach(
        id ->
            assertFalse(
                preExisting.contains(id),
                "should not have created reference to pre-existing snapshot"));
  }

  @Test
  void doNotFailOnMissingSnapshotId() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    JobExecutionContext mockContext =
        stubJobContext(jobId, minimalDataAvroResource, UUID.randomUUID());

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWritePfbStream(any(), any(), any(), eq(BASE_ATTRIBUTES)))
        .thenReturn(BatchWriteResult.empty());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    // Should not call wsm dao
    verify(wsmDao, times(0)).linkSnapshotForPolicy(any());
    // But job should succeed
    verify(jobDao).succeeded(jobId);
  }

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, UUID.randomUUID());

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWritePfbStream(any(), any(), any(), eq(BASE_ATTRIBUTES)))
        .thenReturn(BatchWriteResult.empty());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(wsmDao)
        .linkSnapshotForPolicy(
            ArgumentMatchers.argThat(
                new SnapshotModelMatcher(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5"))));
    // Job should succeed
    verify(jobDao).succeeded(jobId);
  }

  private record SnapshotModelMatcher(UUID expectedSnapshotId)
      implements ArgumentMatcher<SnapshotModel> {

    @Override
    public boolean matches(SnapshotModel exampleSnapshot) {
      return exampleSnapshot.getId().equals(expectedSnapshotId);
    }
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
}
