package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@DirtiesContext
@SpringBootTest
class RawlsSnapshotSupportTest extends ControlPlaneTestBase {

  @MockitoBean RawlsClient rawlsClient;
  @MockitoBean ActivityLogger activityLogger;

  //  @ParameterizedTest(name = "paginates through results when Rawls has {0} references")
  //  @ValueSource(ints = {0, 1, 49, 50, 51, 99, 100, 101, 456})
  //  void paginateExistingSnapshots(int count) {
  //    int testPageSize = 50; // page size to use during this test
  //
  //    List<DataRepoSnapshotResource> mockResources = new ArrayList<>();
  //    // generate the full list of snapshots as known by our mock Rawls
  //    for (int i = 0; i < count; i++) {
  //      DataRepoSnapshotResource resource = new DataRepoSnapshotResource();
  //      DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
  //      dataRepoSnapshotAttributes.setSnapshot(UUID.randomUUID().toString());
  //      dataRepoSnapshotAttributes.setInstanceName("index: " + i);
  //      resource.setAttributes(dataRepoSnapshotAttributes);
  //      mockResources.add(resource);
  //    }
  //    // configure the mock to return the appropriate page of snapshots
  //    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
  //        .thenAnswer(
  //            invocation -> {
  //              int offset = invocation.getArgument(1);
  //              int limit = invocation.getArgument(2);
  //              int sliceEnd =
  //                  Math.min(offset + limit, mockResources.size()); // slice may be a partial page
  //              // calculate the slice to return
  //              List<DataRepoSnapshotResource> slice = mockResources.subList(offset, sliceEnd);
  //              return new SnapshotListResponse(slice);
  //            });
  //
  //    ResourceList actual =
  //        new RawlsSnapshotSupport(WorkspaceId.of(UUID.randomUUID()), rawlsClient, activityLogger)
  //            .listAllSnapshots(testPageSize);
  //
  //    // assert total size of all results
  //    assertEquals(count, actual.getResources().size());
  //    // assert that the "snapshot instance name" (not "WDS instance") is unique in all our
  // results
  //    // i.e. we did not return the same snapshot more than once
  //    List<String> snapshotInstanceNames =
  //        actual.getResources().stream()
  //            .map(res -> res.getResourceAttributes().getGcpDataRepoSnapshot().getInstanceName())
  //            .distinct()
  //            .toList();
  //    assertEquals(count, snapshotInstanceNames.size());
  //    // assert the number of requests made to Rawls to generate the list
  //    double expectedInvocations = Math.floor((double) count / testPageSize) + 1;
  //    verify(rawlsClient, times((int) expectedInvocations))
  //        .enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt());
  //  }
}
