package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.stubJobContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.CloningInstructionsEnum;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.Properties;
import bio.terra.workspace.model.Property;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import bio.terra.workspace.model.ResourceMetadata;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Tag;
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
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext
@SpringBootTest
@ActiveProfiles("mock-sam")
class PfbQuartzJobTest extends TestBase {
  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @MockBean BatchWriteService batchWriteService;
  @MockBean CollectionService collectionService;
  @MockBean ActivityLogger activityLogger;
  @Autowired PfbTestSupport testSupport;

  // test resources used below
  @Value("classpath:avro/minimal_data.avro")
  Resource minimalDataAvroResource;

  @Value("classpath:avro/test.avro")
  Resource testAvroResource;

  @Test
  void linkAllNewSnapshots() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());
    // WSM returns no pre-existing snapshots
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    // call linkSnapshots
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);
    // capture calls
    ArgumentCaptor<SnapshotModel> argumentCaptor = ArgumentCaptor.forClass(SnapshotModel.class);
    // should have called WSM's create-snapshot-reference 10 times
    verify(wsmDao, times(input.size()))
        .linkSnapshotForPolicy(eq(workspaceId), argumentCaptor.capture());
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
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);
    // should not call WSM's create-snapshot-reference at all
    verify(wsmDao, times(0)).linkSnapshotForPolicy(eq(workspaceId), any());
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
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);

    // should call WSM's create-snapshot-reference only for the references that didn't already exist
    int expectedCallCount = input.size() - resourceDescriptions.size();
    ArgumentCaptor<SnapshotModel> argumentCaptor = ArgumentCaptor.forClass(SnapshotModel.class);
    verify(wsmDao, times(expectedCallCount))
        .linkSnapshotForPolicy(eq(workspaceId), argumentCaptor.capture());
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
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    JobExecutionContext mockContext =
        stubJobContext(jobId, minimalDataAvroResource, collectionId.id());

    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // Should not call wsm dao
    verify(wsmDao, times(0)).linkSnapshotForPolicy(eq(workspaceId), any());
    // But job should succeed
    verify(jobDao).succeeded(jobId);
  }

  @Test
  @Tag(SLOW)
  void useWorkspaceIdFromCollection() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, collectionId.id());

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    // specify the workspaceId associated with the target collection
    WorkspaceId expectedWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(expectedWorkspaceId);

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // verify that snapshot operations use the appropriate workspaceId
    verify(wsmDao, times(1))
        .enumerateDataRepoSnapshotReferences(eq(expectedWorkspaceId), anyInt(), anyInt());
    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(wsmDao)
        .linkSnapshotForPolicy(
            eq(expectedWorkspaceId),
            ArgumentMatchers.argThat(
                new SnapshotModelMatcher(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5"))));

    // But job should succeed
    verify(jobDao).succeeded(jobId);
  }

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, collectionId.id());

    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(wsmDao)
        .linkSnapshotForPolicy(
            eq(workspaceId),
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

    ResourceMetadata resourceMetadata = new ResourceMetadata();
    Property purposeProperty = new Property();
    purposeProperty.setKey(WorkspaceManagerDao.PROP_PURPOSE);
    purposeProperty.setValue(WorkspaceManagerDao.PURPOSE_POLICY);
    Properties properties = new Properties();
    properties.add(purposeProperty);
    resourceMetadata.setProperties(properties);
    resourceMetadata.setCloningInstructions(CloningInstructionsEnum.REFERENCE);
    resourceDescription.setMetadata(resourceMetadata);

    return resourceDescription;
  }
}
