package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.TableModel;
import bio.terra.workspace.model.CloningInstructionsEnum;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.Properties;
import bio.terra.workspace.model.Property;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import bio.terra.workspace.model.ResourceMetadata;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class SnapshotSupportTest extends ControlPlaneTestBase {

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
  void extractSnapshotIds() {
    List<UUID> expected = IntStream.range(0, 75).mapToObj(i -> UUID.randomUUID()).toList();

    Stream<ResourceDescription> resourceDescriptions =
        expected.stream().map(this::createResourceDescription);

    List<UUID> actual = defaultSupport().extractSnapshotIds(resourceDescriptions).toList();

    assertEquals(expected, actual);
  }

  @Test
  void existingPolicySnapshotIds() {
    // Arrange
    SnapshotSupport support = spy(defaultSupport());

    UUID policySnapshotId = UUID.randomUUID();
    UUID firstOtherSnapshotId = UUID.randomUUID();
    UUID secondOtherSnapshotId = UUID.randomUUID();

    // This snapshot reference has cloning instructions set to reference and a purpose: policy
    // property.
    ResourceDescription policySnapshotResourceDescription =
        createResourceDescription(policySnapshotId);
    ResourceMetadata policySnapshotResourceMetadata = new ResourceMetadata();
    Property policySnapshotPurposeProperty = new Property();
    policySnapshotPurposeProperty.setKey(SnapshotSupport.PROP_PURPOSE);
    policySnapshotPurposeProperty.setValue(SnapshotSupport.PURPOSE_POLICY);
    Properties policySnapshotProperties = new Properties();
    policySnapshotProperties.add(policySnapshotPurposeProperty);
    policySnapshotResourceMetadata.setProperties(policySnapshotProperties);
    policySnapshotResourceMetadata.setCloningInstructions(CloningInstructionsEnum.REFERENCE);
    policySnapshotResourceDescription.setMetadata(policySnapshotResourceMetadata);

    // This snapshot reference has a purpose: policy property, but cloning instructions set to copy
    // nothing.
    ResourceDescription firstOtherSnapshotResourceDescription =
        createResourceDescription(firstOtherSnapshotId);
    ResourceMetadata firstOtherSnapshotResourceMetadata = new ResourceMetadata();
    Property firstOtherSnapshotPurposeProperty = new Property();
    firstOtherSnapshotPurposeProperty.setKey(SnapshotSupport.PROP_PURPOSE);
    firstOtherSnapshotPurposeProperty.setValue(SnapshotSupport.PURPOSE_POLICY);
    Properties firstOtherSnapshotProperties = new Properties();
    firstOtherSnapshotProperties.add(firstOtherSnapshotPurposeProperty);
    firstOtherSnapshotResourceMetadata.setProperties(firstOtherSnapshotProperties);
    firstOtherSnapshotResourceMetadata.setCloningInstructions(CloningInstructionsEnum.NOTHING);
    firstOtherSnapshotResourceDescription.setMetadata(firstOtherSnapshotResourceMetadata);

    // This snapshot reference has no metadata at all
    ResourceDescription secondOtherSnapshotResourceDescription =
        createResourceDescription(secondOtherSnapshotId);

    ResourceList resourceList = new ResourceList();
    resourceList.setResources(
        List.of(
            policySnapshotResourceDescription,
            firstOtherSnapshotResourceDescription,
            secondOtherSnapshotResourceDescription));

    when(support.enumerateDataRepoSnapshotReferences(anyInt(), anyInt())).thenReturn(resourceList);

    // Act
    List<UUID> policySnapshotIds = support.existingPolicySnapshotIds(5);

    // Assert
    assertEquals(List.of(policySnapshotId), policySnapshotIds);
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

  // No need for these methods to implemented, this is used only for testing shared behavior in the
  // abstract class
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
