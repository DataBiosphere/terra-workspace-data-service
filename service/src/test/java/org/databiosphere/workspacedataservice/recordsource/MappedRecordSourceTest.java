package org.databiosphere.workspacedataservice.recordsource;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class MappedRecordSourceTest {
  @Test
  void mapsRecords() throws IOException {
    // Arrange
    var recordType = RecordType.valueOf("record");
    var records =
        List.of(
            new Record("1", recordType, new RecordAttributes(Map.of("foo", 1, "bar", 2), "1")),
            new Record("2", recordType, new RecordAttributes(Map.of("foo", 2, "bar", 4), "2")),
            new Record("3", recordType, new RecordAttributes(Map.of("foo", 3, "bar", 6), "3")));

    MapRecordFunction mapRecord =
        record ->
            record.putAllAttributes(
                new RecordAttributes(Map.of("baz", (int) record.getAttributeValue("foo") * 3)));

    var originalSource = new StaticRecordSource(records);
    var mappedSource = new MappedRecordSource(originalSource, mapRecord);

    // Act
    var output = mappedSource.readRecords(3);

    // Assert
    assertThat(output.records())
        .isEqualTo(
            List.of(
                new Record(
                    "1",
                    recordType,
                    new RecordAttributes(Map.of("foo", 1, "bar", 2, "baz", 3), "1")),
                new Record(
                    "2",
                    recordType,
                    new RecordAttributes(Map.of("foo", 2, "bar", 4, "baz", 6), "2")),
                new Record(
                    "3",
                    recordType,
                    new RecordAttributes(Map.of("foo", 3, "bar", 6, "baz", 9), "3"))));
  }

  private static class StaticRecordSource implements RecordSource {
    private final List<Record> records;
    private int index = 0;

    public StaticRecordSource(List<Record> records) {
      this.records = records;
    }

    @Override
    public WriteStreamInfo readRecords(int numRecords) {
      if (index >= records.size()) {
        return new WriteStreamInfo(emptyList(), OperationType.UPSERT);
      }
      List<Record> output = records.subList(index, Math.min(index + numRecords, records.size()));
      index += numRecords;
      return new WriteStreamInfo(output, OperationType.UPSERT);
    }

    @Override
    public void close() {}
  }
}
