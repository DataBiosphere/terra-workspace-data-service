package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.stubJobContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
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
 * This test explicitly asserts on and mocks calls to Rawls as part of PFB import; it is explicitly
 * a test of control-plane behavior.
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

    // call linkSnapshots
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    PfbQuartzJob pfbQuartzJob = testSupport.buildPfbQuartzJob();
    pfbQuartzJob.linkSnapshots(input, workspaceId);
    // capture calls
    ArgumentCaptor<List<UUID>> argumentCaptor = ArgumentCaptor.forClass(List.class);
    // should have called Rawls's create-snapshot-reference
    verify(rawlsClient, times(1))
        .createSnapshotReferences(eq(workspaceId.id()), argumentCaptor.capture());
    // those 10 calls should have used our 10 input UUIDs
    List<UUID> actualSnapshotIds = argumentCaptor.getValue();
    Set<UUID> actualUuids = new HashSet<>(actualSnapshotIds);
    assertEquals(input, actualUuids);
  }

  @Test
  void doNotFailOnMissingSnapshotId() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    JobExecutionContext mockContext =
        stubJobContext(jobId, minimalDataAvroResource, collectionId.id());

    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // Should not call Rawls client
    verify(rawlsClient, times(0)).createSnapshotReferences(eq(workspaceId.id()), any());
    // Job should be running (can't test succeeded because that needs a message back from Rawls)
    verify(jobDao).running(jobId);
  }

  @Test
  @Tag(SLOW)
  void useWorkspaceIdFromCollection() throws JobExecutionException, IOException {
    UUID jobId = UUID.randomUUID();
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, collectionId.id());

    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    // specify the workspaceId associated with the target collection
    WorkspaceId expectedWorkspaceId = WorkspaceId.of(UUID.randomUUID());
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(expectedWorkspaceId);

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // verify that snapshot operations use the appropriate workspaceId
    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(rawlsClient)
        .createSnapshotReferences(
            expectedWorkspaceId.id(),
            List.of(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5")));

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

    // We're not testing this, so it doesn't matter what returns
    when(batchWriteService.batchWrite(any(), any(), any(), any()))
        .thenReturn(BatchWriteResult.empty());

    testSupport.buildPfbQuartzJob().execute(mockContext);

    // The "790795c4..." UUID below is the snapshotId found in the "test.avro" resource used
    // by this unit test
    verify(rawlsClient)
        .createSnapshotReferences(
            workspaceId.id(), List.of(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5")));
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
}
