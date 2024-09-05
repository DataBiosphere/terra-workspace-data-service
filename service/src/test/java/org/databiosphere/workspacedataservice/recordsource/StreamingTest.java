package org.databiosphere.workspacedataservice.recordsource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.shared.model.OperationType.DELETE;
import static org.databiosphere.workspacedataservice.shared.model.OperationType.UPSERT;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.WriteStreamInfo;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class StreamingTest extends DataPlaneTestBase {

  @Autowired private ObjectMapper objectMapper;

  @Test
  void testReadLessThanWholeStream() throws IOException {
    RecordSource source =
        new JsonRecordSource(
            StreamingTest.class.getResourceAsStream("/batch-write/upsert.json"), objectMapper);
    List<Record> records = source.readRecords(1).records();
    assertThat(records).as("Should only read 1 out of 2 records in the file").hasSize(1);
  }

  @Test
  void testReadWholeStream() throws IOException {
    RecordSource source =
        new JsonRecordSource(
            StreamingTest.class.getResourceAsStream("/batch-write/upsert.json"), objectMapper);
    List<Record> records = source.readRecords(500).records();
    assertThat(records).as("Should read all 2 records in the file").hasSize(2);
  }

  @Test
  void testReadMixedOperations() throws IOException {
    RecordSource source =
        new JsonRecordSource(
            StreamingTest.class.getResourceAsStream("/batch-write/mix.json"), objectMapper);
    WriteStreamInfo res = source.readRecords(500);
    assertThat(res.records()).as("Should read 1 record").hasSize(1);
    assertThat(res.operationType()).isEqualTo(UPSERT);
    res = source.readRecords(500);
    assertThat(res.records()).as("Should read 1 record").hasSize(1);
    assertThat(res.operationType()).isEqualTo(DELETE);
    res = source.readRecords(500);
    assertThat(res.records()).as("Should read 1 record").hasSize(1);
    assertThat(res.operationType()).isEqualTo(UPSERT);
  }
}
