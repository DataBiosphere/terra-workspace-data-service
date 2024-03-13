package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class SnapshotSupportTest extends TestBase {

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
    SnapshotSupport support = defaultSupport();
    assertEquals("datarepo_row_id", support.getDefaultPrimaryKey());
  }

  @Test
  void nullPrimaryKey() {
    SnapshotSupport support = defaultSupport();
    String actual = support.identifyPrimaryKey(null);
    assertEquals(support.getDefaultPrimaryKey(), actual);
  }

  @Test
  void missingPrimaryKey() {
    SnapshotSupport support = defaultSupport();
    String actual = support.identifyPrimaryKey(List.of());
    assertEquals(support.getDefaultPrimaryKey(), actual);
  }

  @Test
  void multiplePrimaryKeys() {
    SnapshotSupport support = defaultSupport();
    String actual = support.identifyPrimaryKey(List.of("one", "two"));
    assertEquals(support.getDefaultPrimaryKey(), actual);
  }

  @Test
  void singlePrimaryKey() {
    SnapshotSupport support = defaultSupport();
    String expected = "my_primary_key";
    String actual = support.identifyPrimaryKey(List.of(expected));
    assertEquals(expected, actual);
  }

  @Test
  void singleRandomPrimaryKey() {
    SnapshotSupport support = defaultSupport();
    String expected = RandomStringUtils.randomPrint(16);
    String actual = support.identifyPrimaryKey(List.of(expected));
    assertEquals(expected, actual);
  }

  @Test
  void primaryKeysForAllTables() {
    SnapshotSupport support = defaultSupport();
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

  private SnapshotSupport defaultSupport() {
    return new SnapshotSupport() {
      protected void linkSnapshot(UUID snapshotId) {
        // no-op
      }

      protected ResourceList enumerateDataRepoSnapshotReferences(int offset, int limit) {
        return new ResourceList();
      }
    };
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
