package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.datarepo.model.TableModel;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

      public List<UUID> existingPolicySnapshotIds(int pageSize) {
        return List.of();
      }
    };
  }
}
