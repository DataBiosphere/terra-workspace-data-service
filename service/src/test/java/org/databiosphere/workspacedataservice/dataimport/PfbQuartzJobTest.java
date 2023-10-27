package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class PfbQuartzJobTest {

  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @Autowired RestClientRetry restClientRetry;

  @ParameterizedTest(name = "paginates through results when WSM has {0} references")
  @ValueSource(ints = {0, 1, 49, 50, 51, 99, 100, 101, 456})
  void paginateExistingSnapshots(int wsmCount) throws ApiException {
    int testPageSize = 50; // page size to use during this test

    List<ResourceDescription> mockResources = new ArrayList<>();
    // generate the full list of snapshots as known by our mock WSM
    for (int i = 0; i < wsmCount; i++) {
      ResourceDescription resourceDescription = new ResourceDescription();
      ResourceAttributesUnion resourceAttributesUnion = new ResourceAttributesUnion();
      DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
      dataRepoSnapshotAttributes.setSnapshot(UUID.randomUUID().toString());
      dataRepoSnapshotAttributes.setInstanceName("index: " + i);
      resourceAttributesUnion.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);
      resourceDescription.setResourceAttributes(resourceAttributesUnion);
      mockResources.add(resourceDescription);
    }
    // configure the mock to return the appropriate page of snapshots
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenAnswer(
            invocation -> {
              int offset = invocation.getArgument(1);
              int limit = invocation.getArgument(2);
              int sliceEnd =
                  Math.min(offset + limit, mockResources.size()); // slice may be a partial page
              // calculate the slice to return
              List<ResourceDescription> slice = mockResources.subList(offset, sliceEnd);
              ResourceList resourceList = new ResourceList();
              resourceList.setResources(slice);
              return resourceList;
            });

    ResourceList actual =
        PfbQuartzJobSupport.listAllSnapshots(
            UUID.randomUUID(), testPageSize, wsmDao, restClientRetry);

    // assert total size of all results
    assertEquals(wsmCount, actual.getResources().size());
    // assert that the "snapshot instance name" (not "WDS instance") is unique in all our results
    // i.e. we did not return the same snapshot more than once
    List<String> snapshotInstanceNames =
        actual.getResources().stream()
            .map(res -> res.getResourceAttributes().getGcpDataRepoSnapshot().getInstanceName())
            .distinct()
            .toList();
    assertEquals(wsmCount, snapshotInstanceNames.size());
    // assert the number of requests made to WSM to generate the list
    double expectedInvocations = Math.floor((double) wsmCount / testPageSize) + 1;
    verify(wsmDao, times((int) expectedInvocations))
        .enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt());
  }

  @Test
  void safeGetSnapshotId() {
    UUID snapshotId = UUID.randomUUID();
    ResourceDescription resourceDescription = createResourceDescription(snapshotId);

    UUID actual = PfbQuartzJobSupport.safeGetSnapshotId(resourceDescription);

    assertEquals(snapshotId, actual);
  }

  @Test
  void safeGetSnapshotIdNonUuid() {
    String notAUuid = "Hello world";

    DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
    dataRepoSnapshotAttributes.setSnapshot(notAUuid);

    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    UUID actual = PfbQuartzJobSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNull() {
    DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
    dataRepoSnapshotAttributes.setSnapshot(null);

    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    UUID actual = PfbQuartzJobSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoSnapshotObject() {
    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(null);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    UUID actual = PfbQuartzJobSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoAttributes() {
    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(null);

    UUID actual = PfbQuartzJobSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void existingPolicySnapshotIds() {
    List<UUID> expected = IntStream.range(0, 75).mapToObj(i -> UUID.randomUUID()).toList();

    List<ResourceDescription> resourceDescriptions =
        expected.stream().map(this::createResourceDescription).toList();

    ResourceList resourceList = new ResourceList();
    resourceList.setResources(resourceDescriptions);

    List<UUID> actual = PfbQuartzJobSupport.extractSnapshotIds(resourceList);

    assertEquals(expected, actual);
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
