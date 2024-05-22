package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class TdrManifestImportMetadataTest {
  @Test
  void getAddToRecordFunction() {
    // Arrange
    var recordType = RecordType.valueOf("thing");
    var record = new Record("1", recordType, new RecordAttributes(Map.of("value", 1), "1"));

    var snapshotId = UUID.randomUUID();
    var importTime = Instant.ofEpochSecond(1716335100);

    var importMetadata = new TdrManifestImportMetadata(snapshotId, importTime);

    // Act
    var mapRecord = importMetadata.getAddToRecordFunction();
    var annotatedRecord = mapRecord.apply(record);

    // Assert
    assertThat(annotatedRecord)
        .isEqualTo(
            new Record(
                "1",
                recordType,
                new RecordAttributes(
                    Map.of(
                        "value",
                        1,
                        "import:snapshot_id",
                        snapshotId.toString(),
                        "import:timestamp",
                        "05-21-2024T23:45:00"),
                    "1")));
  }
}
