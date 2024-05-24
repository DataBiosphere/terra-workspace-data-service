package org.databiosphere.workspacedataservice.dataimport.tdr;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;

public class TdrManifestImportMetadata {
  public static final String IMPORT_METADATA_PREFIX = "import:";
  private final UUID snapshotId;
  private final Instant importTime;

  public TdrManifestImportMetadata(UUID snapshotId, Instant importTime) {
    this.snapshotId = snapshotId;
    this.importTime = importTime;
  }

  public UnaryOperator<Record> getAddToRecordFunction() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    String timestamp = ZonedDateTime.ofInstant(importTime, ZoneId.of("UTC")).format(formatter);

    Map<String, Object> attributes =
        Map.of(
            "%stimestamp".formatted(IMPORT_METADATA_PREFIX),
            timestamp,
            "%ssnapshot_id".formatted(IMPORT_METADATA_PREFIX),
            snapshotId.toString());

    return record -> record.putAllAttributes(new RecordAttributes(attributes));
  }
}
