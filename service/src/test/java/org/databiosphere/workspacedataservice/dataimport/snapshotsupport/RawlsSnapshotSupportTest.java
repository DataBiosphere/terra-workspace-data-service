package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import bio.terra.workspace.model.ResourceList;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
@SpringBootTest
class RawlsSnapshotSupportTest extends DataPlaneTestBase {

  @MockBean RawlsClient rawlsClient;
  @MockBean ActivityLogger activityLogger;

  @ParameterizedTest(name = "paginates through results when Rawls has {0} references")
  @ValueSource(ints = {0, 1, 49, 50, 51, 99, 100, 101, 456})
  void paginateExistingSnapshots(int count) {
    int testPageSize = 50; // page size to use during this test

    List<DataRepoSnapshotResource> mockResources = new ArrayList<>();
    // generate the full list of snapshots as known by our mock Rawls
    for (int i = 0; i < count; i++) {
      DataRepoSnapshotResource resource = new DataRepoSnapshotResource();
      DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
      dataRepoSnapshotAttributes.setSnapshot(UUID.randomUUID().toString());
      dataRepoSnapshotAttributes.setInstanceName("index: " + i);
      resource.setAttributes(dataRepoSnapshotAttributes);
      mockResources.add(resource);
    }
    // configure the mock to return the appropriate page of snapshots
    when(rawlsClient.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenAnswer(
            invocation -> {
              int offset = invocation.getArgument(1);
              int limit = invocation.getArgument(2);
              int sliceEnd =
                  Math.min(offset + limit, mockResources.size()); // slice may be a partial page
              // calculate the slice to return
              List<DataRepoSnapshotResource> slice = mockResources.subList(offset, sliceEnd);
              return new SnapshotListResponse(slice);
            });

    ResourceList actual =
        new RawlsSnapshotSupport(WorkspaceId.of(UUID.randomUUID()), rawlsClient, activityLogger)
            .listAllSnapshots(testPageSize);

    // assert total size of all results
    assertEquals(count, actual.getResources().size());
    // assert that the "snapshot instance name" (not "WDS instance") is unique in all our results
    // i.e. we did not return the same snapshot more than once
    List<String> snapshotInstanceNames =
        actual.getResources().stream()
            .map(res -> res.getResourceAttributes().getGcpDataRepoSnapshot().getInstanceName())
            .distinct()
            .toList();
    assertEquals(count, snapshotInstanceNames.size());
    // assert the number of requests made to Rawls to generate the list
    double expectedInvocations = Math.floor((double) count / testPageSize) + 1;
    verify(rawlsClient, times((int) expectedInvocations))
        .enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt());
  }
}
