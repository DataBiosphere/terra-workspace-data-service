package org.databiosphere.workspacedataservice.recordstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.shared.model.OperationType.DELETE;
import static org.databiosphere.workspacedataservice.shared.model.OperationType.UPSERT;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.BatchWriteService.WriteStreamInfo;
import org.databiosphere.workspacedataservice.service.JsonConfig;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest(classes = JsonConfig.class)
class StreamingTest {

  @Autowired private ObjectMapper objectMapper;

  @Test
  void testReadLessThanWholeStream() throws IOException {
    BatchWriteService.RecordSource handler =
        new JsonRecordSource(
            StreamingTest.class.getResourceAsStream("/batch-write/upsert.json"), objectMapper);
    List<Record> records = handler.readRecords(1).records();
    assertThat(records).as("Should only read 1 out of 2 records in the file").hasSize(1);
  }

  @Test
  void testReadWholeStream() throws IOException {
    BatchWriteService.RecordSource handler =
        new JsonRecordSource(
            StreamingTest.class.getResourceAsStream("/batch-write/upsert.json"), objectMapper);
    List<Record> records = handler.readRecords(500).records();
    assertThat(records).as("Should read all 2 records in the file").hasSize(2);
  }

  @Test
  void testReadMixedOperations() throws IOException {
    BatchWriteService.RecordSource handler =
        new JsonRecordSource(
            StreamingTest.class.getResourceAsStream("/batch-write/mix.json"), objectMapper);
    WriteStreamInfo res = handler.readRecords(500);
    assertThat(res.records()).as("Should read 1 record").hasSize(1);
    assertThat(res.operationType()).isEqualTo(UPSERT);
    res = handler.readRecords(500);
    assertThat(res.records()).as("Should read 1 record").hasSize(1);
    assertThat(res.operationType()).isEqualTo(DELETE);
    res = handler.readRecords(500);
    assertThat(res.records()).as("Should read 1 record").hasSize(1);
    assertThat(res.operationType()).isEqualTo(UPSERT);
  }
}
