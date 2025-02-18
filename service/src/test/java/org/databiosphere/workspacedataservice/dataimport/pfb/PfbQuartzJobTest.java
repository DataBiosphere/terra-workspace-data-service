package org.databiosphere.workspacedataservice.dataimport.pfb;

import static bio.terra.workspace.model.CloningInstructionsEnum.REFERENCE;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.stubJobContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import bio.terra.workspace.model.Properties;
import bio.terra.workspace.model.Property;
import bio.terra.workspace.model.ResourceMetadata;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * This test explicitly asserts on and mock calls to Rawls as part of PFB import; it is explicitly a
 * test of control-plane behavior.
 */
@DirtiesContext
@SpringBootTest
class PfbQuartzJobTest extends ControlPlaneTestBase {
  @MockitoBean JobDao jobDao;
  @MockitoBean RawlsClient rawlsClient;
  @MockitoBean BatchWriteService batchWriteService;
  @MockitoBean CollectionService collectionService;
  @MockitoBean DataTableTypeInspector dataTableTypeInspector;
  @Autowired PfbTestSupport testSupport;
  @Autowired MeterRegistry meterRegistry;

  // test resources used below
  @Value("classpath:avro/minimal_data.avro")
  Resource minimalDataAvroResource;

  @Value("classpath:avro/test.avro")
  Resource testAvroResource;

  @BeforeEach
  void beforeEach() {
    // dataTableTypeInspector says ok to use data tables
    when(dataTableTypeInspector.getWorkspaceDataTableType(any()))
        .thenReturn(WorkspaceDataTableType.WDS);
  }

  @Test
  void linkAllNewSnapshots() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());
    // Rawls returns no pre-existing snapshots
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(List.of()));

    // call linkSnapshots
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);
    // capture calls
    ArgumentCaptor<UUID> argumentCaptor = ArgumentCaptor.forClass(UUID.class);
    // should have called Rawls's create-snapshot-reference 10 times
    verify(rawlsClient, times(input.size()))
        .createSnapshotReference(eq(workspaceId.id()), argumentCaptor.capture());
    // those 10 calls should have used our 10 input UUIDs
    List<UUID> actualSnapshotIds = argumentCaptor.getAllValues();
    Set<UUID> actualUuids = new HashSet<>(actualSnapshotIds);
    assertEquals(input, actualUuids);
  }

  @Test
  void linkNothingWhenAllExist() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());

    // Rawls returns all of those UUIDs as pre-existing snapshots
    List<DataRepoSnapshotResource> snapshotResources = generateSnapshotResources(input);
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(snapshotResources));

    // call linkSnapshots
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);
    // should not call Rawls's create-snapshot-reference at all
    verify(rawlsClient, times(0)).createSnapshotReference(eq(workspaceId.id()), any());
  }

  @Test
  void linkSomeWhenSomeExist() {
    // input is a list of 10 UUIDs
    Set<UUID> input =
        IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toSet());

    // Rawls returns some of those UUIDs as pre-existing snapshots
    Random random = new Random();
    // note that the random in here can select the same UUID twice from the input, thus
    // we need the distinct()
    List<UUID> preExisting =
        IntStream.range(0, 5)
            .mapToObj(i -> input.stream().toList().get(random.nextInt(input.size())))
            .distinct()
            .toList();
    List<DataRepoSnapshotResource> snapshotResources = generateSnapshotResources(preExisting);

    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(snapshotResources));

    // call linkSnapshots
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);

    // should call Rawls's create-snapshot-reference only for the references that didn't already
    // exist
    int expectedCallCount = input.size() - snapshotResources.size();
    ArgumentCaptor<UUID> argumentCaptor = ArgumentCaptor.forClass(UUID.class);
    verify(rawlsClient, times(expectedCallCount))
        .createSnapshotReference(eq(workspaceId.id()), argumentCaptor.capture());
    List<UUID> actual = argumentCaptor.getAllValues().stream().toList();
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

    // Rawls should report no snapshots already linked to this workspace
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(List.of()));
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // Should not call Rawls client
    verify(rawlsClient, times(0)).createSnapshotReference(eq(workspaceId.id()), any());
    // Job should be running (can't test succeeded because that needs a message back from Rawls)
    verify(jobDao).running(jobId);
  }

  @Test
  @Tag(SLOW)
  void useWorkspaceIdFromCollection() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, collectionId.id());

    // Rawls should report no snapshots already linked to this workspace
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(List.of()));
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    // specify the workspaceId associated with the target collection
    WorkspaceId expectedWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(expectedWorkspaceId);

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // verify that snapshot operations use the appropriate workspaceId
    verify(rawlsClient, times(1))
        .enumerateDataRepoSnapshotReferences(eq(expectedWorkspaceId.id()), anyInt(), anyInt());
    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(rawlsClient)
        .createSnapshotReference(
            eq(expectedWorkspaceId.id()),
            eq(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5")));

    // Job should be running (can't test succeeded because that needs a message back from Rawls)
    verify(jobDao).running(jobId);
  }

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, collectionId.id());

    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // Rawls should report no snapshots already linked to this workspace
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(List.of()));
    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(rawlsClient)
        .createSnapshotReference(
            eq(workspaceId.id()), eq(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5")));
    // Job should be running (can't test succeeded because that needs a message back from Rawls)
    verify(jobDao).running(jobId);
  }

  @Test
  void upsertCountMetricsAreRecorded() throws IOException, JobExecutionException {
    // get the starting state of the distribution summaries
    // since other test cases may write metrics, we can't predict their starting state
    DistributionSummary upsertCountSummary = meterRegistry.find("wds.import.upsertCount").summary();
    assertNotNull(upsertCountSummary);
    long startingUpsertCount = upsertCountSummary.count();
    double startingUpsertTotal = upsertCountSummary.totalAmount();

    DistributionSummary snapshotsConsideredSummary =
        meterRegistry.find("wds.import.snapshotsConsidered").summary();
    assertNotNull(snapshotsConsideredSummary);
    long startingSnapshotsConsideredCount = snapshotsConsideredSummary.count();
    double startingSnapshotsConsideredTotal = snapshotsConsideredSummary.totalAmount();

    DistributionSummary snapshotsLinkedSummary =
        meterRegistry.find("wds.import.snapshotsLinked").summary();
    assertNotNull(snapshotsLinkedSummary);
    long startingSnapshotsLinkedCount = snapshotsLinkedSummary.count();
    double startingSnapshotsLinkedTotal = snapshotsLinkedSummary.totalAmount();

    // set up the mock import
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    JobExecutionContext mockContext =
        stubJobContext(jobId, minimalDataAvroResource, collectionId.id());

    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // Rawls should report no snapshots already linked to this workspace
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new SnapshotListResponse(List.of()));
    // BatchWriteService returns a BatchWriteResult of 1234 upserts
    BatchWriteResult result = BatchWriteResult.empty();
    result.increaseCount(RecordType.valueOf("fooType"), 1234);
    when(batchWriteService.batchWrite(any(), any(), any(), any())).thenReturn(result);

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // Job should be running (can't test succeeded because that needs a message back from Rawls)
    verify(jobDao).running(jobId);

    // get the ending state of the upsertCount distribution summary, now that we've run a job
    long endingUpsertCount = upsertCountSummary.count();
    double endingUpsertTotal = upsertCountSummary.totalAmount();
    // we should have incremented the summary count by 1
    assertEquals(1, endingUpsertCount - startingUpsertCount);
    // and we should have incremented the total by 2468.
    // our mock returns a BatchWriteResult of 1234, but import has two passes - base attributes
    // and relations. Thus, it is expected that each pass will return 1234 and the expected metric
    // is (1234 * 2), which is 2468.
    assertEquals(2468, endingUpsertTotal - startingUpsertTotal);

    // The PFB being imported does not mention any snapshots. So, we should see the *count* for
    // these metrics increase, but the *total* should stay the same.
    long endingSnapshotsConsideredCount = snapshotsConsideredSummary.count();
    double endingSnapshotsConsideredTotal = snapshotsConsideredSummary.totalAmount();
    assertEquals(1, endingSnapshotsConsideredCount - startingSnapshotsConsideredCount);
    assertEquals(0, endingSnapshotsConsideredTotal - startingSnapshotsConsideredTotal);

    long endingSnapshotsLinkedCount = snapshotsLinkedSummary.count();
    double endingSnapshotsLinkedTotal = snapshotsLinkedSummary.totalAmount();
    assertEquals(1, endingSnapshotsLinkedCount - startingSnapshotsLinkedCount);
    assertEquals(0, endingSnapshotsLinkedTotal - startingSnapshotsLinkedTotal);
  }

  private List<DataRepoSnapshotResource> generateSnapshotResources(Collection<UUID> input) {
    return input.stream()
        .map(
            id -> {
              Property p = new Property();
              p.setKey(WorkspaceManagerDao.PROP_PURPOSE);
              p.setValue(WorkspaceManagerDao.PURPOSE_POLICY);
              Properties properties = new Properties();
              properties.add(p);
              ResourceMetadata resourceMetadata = new ResourceMetadata();
              resourceMetadata.setResourceId(UUID.randomUUID());
              resourceMetadata.setProperties(properties);
              resourceMetadata.setCloningInstructions(REFERENCE);
              DataRepoSnapshotAttributes attributes = new DataRepoSnapshotAttributes();
              attributes.setSnapshot(id.toString());
              return new DataRepoSnapshotResource()
                  .metadata(resourceMetadata)
                  .attributes(attributes);
            })
        .collect(Collectors.toList());
  }
}
