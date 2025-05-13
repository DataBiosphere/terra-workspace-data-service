package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class SnapshotSupportTest extends ControlPlaneTestBase {

  private SnapshotSupport snapshotSupport;
  private final RawlsClient rawlsClient = mock(RawlsClient.class);
  private final WorkspaceId workspaceId = new WorkspaceId(UUID.randomUUID());

  @BeforeEach
  void setUp() {
    ActivityLogger activityLogger = mock(ActivityLogger.class);

    snapshotSupport = new SnapshotSupport(workspaceId, rawlsClient, activityLogger);
  }

  @Test
  void safeGetSnapshotId() {
    UUID snapshotId = UUID.randomUUID();
    ResourceDescription resourceDescription = createResourceDescription(snapshotId);

    UUID actual = snapshotSupport.safeGetSnapshotId(resourceDescription);

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

    UUID actual = snapshotSupport.safeGetSnapshotId(resourceDescription);

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

    UUID actual = snapshotSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoSnapshotObject() {
    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(null);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    UUID actual = snapshotSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoAttributes() {
    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(null);

    UUID actual = snapshotSupport.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void extractSnapshotIds() {
    List<UUID> expected = IntStream.range(0, 75).mapToObj(i -> UUID.randomUUID()).toList();

    Stream<ResourceDescription> resourceDescriptions =
        expected.stream().map(this::createResourceDescription);

    List<UUID> actual = snapshotSupport.extractSnapshotIds(resourceDescriptions).toList();

    assertEquals(expected, actual);
  }

  @Test
  void defaultPrimaryKey() {
    assertEquals("datarepo_row_id", snapshotSupport.getDefaultPrimaryKey());
  }

  @Test
  void nullPrimaryKey() {
    String actual = snapshotSupport.identifyPrimaryKey(null);
    assertEquals(snapshotSupport.getDefaultPrimaryKey(), actual);
  }

  @Test
  void missingPrimaryKey() {
    String actual = snapshotSupport.identifyPrimaryKey(List.of());
    assertEquals(snapshotSupport.getDefaultPrimaryKey(), actual);
  }

  @Test
  void multiplePrimaryKeys() {
    String actual = snapshotSupport.identifyPrimaryKey(List.of("one", "two"));
    assertEquals(snapshotSupport.getDefaultPrimaryKey(), actual);
  }

  @Test
  void singlePrimaryKey() {
    String expected = "my_primary_key";
    String actual = snapshotSupport.identifyPrimaryKey(List.of(expected));
    assertEquals(expected, actual);
  }

  @Test
  void singleRandomPrimaryKey() {
    String expected = RandomStringUtils.randomPrint(16);
    String actual = snapshotSupport.identifyPrimaryKey(List.of(expected));
    assertEquals(expected, actual);
  }

  @Test
  void primaryKeysForAllTables() {
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
    Map<RecordType, String> actual = snapshotSupport.identifyPrimaryKeys(input);
    assertEquals(expected, actual);
  }

  @Test
  void linkSnapshots() {
    Set<UUID> snapshotIds = Set.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    boolean result = snapshotSupport.linkSnapshots(snapshotIds);
    assertTrue(result);
    verify(rawlsClient).createSnapshotReferences(workspaceId.id(), snapshotIds.stream().toList());
  }

  @Test
  void doNotLinkEmptySnapshots() {
    Set<UUID> snapshotIds = Set.of();
    boolean result = snapshotSupport.linkSnapshots(snapshotIds);
    assertTrue(result);
    verify(rawlsClient, never()).createSnapshotReferences(any(), any());
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
