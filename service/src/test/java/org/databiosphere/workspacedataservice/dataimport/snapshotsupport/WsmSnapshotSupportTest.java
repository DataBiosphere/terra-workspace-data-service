package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
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
class WsmSnapshotSupportTest extends TestBase {

  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @MockBean ActivityLogger activityLogger;
  @Autowired RestClientRetry restClientRetry;

  @ParameterizedTest(name = "paginates through results when WSM has {0} references")
  @ValueSource(ints = {0, 1, 49, 50, 51, 99, 100, 101, 456})
  void paginateExistingSnapshots(int wsmCount) {
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
        new WsmSnapshotSupport(
                WorkspaceId.of(UUID.randomUUID()), wsmDao, restClientRetry, activityLogger)
            .listAllSnapshots(testPageSize);

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

    UUID actual = defaultSupport().safeGetSnapshotId(resourceDescription);

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

    UUID actual = defaultSupport().safeGetSnapshotId(resourceDescription);

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

    UUID actual = defaultSupport().safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoSnapshotObject() {
    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(null);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    UUID actual = defaultSupport().safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoAttributes() {
    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(null);

    UUID actual = defaultSupport().safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void existingPolicySnapshotIds() {
    List<UUID> expected = IntStream.range(0, 75).mapToObj(i -> UUID.randomUUID()).toList();

    List<ResourceDescription> resourceDescriptions =
        expected.stream().map(this::createResourceDescription).toList();

    ResourceList resourceList = new ResourceList();
    resourceList.setResources(resourceDescriptions);

    List<UUID> actual = defaultSupport().extractSnapshotIds(resourceList);

    assertEquals(expected, actual);
  }

  @Test
  void defaultPrimaryKey() {
    WsmSnapshotSupport support = defaultSupport();
    assertEquals("datarepo_row_id", support.getDefaultPrimaryKey());
  }

  @Test
  void nullPrimaryKey() {
    WsmSnapshotSupport support = defaultSupport();
    String actual = support.identifyPrimaryKey(null);
    assertEquals(support.getDefaultPrimaryKey(), actual);
  }

  @Test
  void missingPrimaryKey() {
    WsmSnapshotSupport support = defaultSupport();
    String actual = support.identifyPrimaryKey(List.of());
    assertEquals(support.getDefaultPrimaryKey(), actual);
  }

  @Test
  void multiplePrimaryKeys() {
    WsmSnapshotSupport support = defaultSupport();
    String actual = support.identifyPrimaryKey(List.of("one", "two"));
    assertEquals(support.getDefaultPrimaryKey(), actual);
  }

  @Test
  void singlePrimaryKey() {
    WsmSnapshotSupport support = defaultSupport();
    String expected = "my_primary_key";
    String actual = support.identifyPrimaryKey(List.of(expected));
    assertEquals(expected, actual);
  }

  @Test
  void singleRandomPrimaryKey() {
    WsmSnapshotSupport support = defaultSupport();
    String expected = RandomStringUtils.randomPrint(16);
    String actual = support.identifyPrimaryKey(List.of(expected));
    assertEquals(expected, actual);
  }

  @Test
  void primaryKeysForAllTables() {
    WsmSnapshotSupport support = defaultSupport();
    List<TableModel> input =
        List.of(
            new TableModel().name("table1").primaryKey(List.of()),
            new TableModel().name("table2").primaryKey(List.of("pk2")),
            new TableModel().name("table3").primaryKey(List.of("pk3", "anotherpk")));
    Map<RecordType, String> expected =
        Map.of(
            RecordType.valueOf("table1"), "datarepo_row_id",
            RecordType.valueOf("table2"), "pk2",
            RecordType.valueOf("table3"), "datarepo_row_id");
    Map<RecordType, String> actual = support.identifyPrimaryKeys(input);
    assertEquals(expected, actual);
  }

  private WsmSnapshotSupport defaultSupport() {
    return new WsmSnapshotSupport(
        WorkspaceId.of(UUID.randomUUID()), wsmDao, restClientRetry, activityLogger);
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
