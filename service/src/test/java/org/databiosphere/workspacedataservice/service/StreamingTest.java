package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.shared.model.OperationType.DELETE;
import static org.databiosphere.workspacedataservice.shared.model.OperationType.UPSERT;

@SpringBootTest
class StreamingTest {

	@Test
	void testReadLessThanWholeStream() throws IOException {
		StreamingWriteHandler handler = new StreamingWriteHandler(
				StreamingTest.class.getResourceAsStream("/batch_write_upsert.json"));
		List<Record> records = handler.readRecords(1).getRecords();
		assertThat(records).as("Should only read 1 out of 2 records in the file").hasSize(1);
	}

	@Test
	void testReadWholeStream() throws IOException {
		StreamingWriteHandler handler = new StreamingWriteHandler(
				StreamingTest.class.getResourceAsStream("/batch_write_upsert.json"));
		List<Record> records = handler.readRecords(500).getRecords();
		assertThat(records).as("Should read all 2 records in the file").hasSize(2);
	}

	@Test
	void testReadMixedOperations() throws IOException {
		StreamingWriteHandler handler = new StreamingWriteHandler(
				StreamingTest.class.getResourceAsStream("/batch_write_mix.json"));
		StreamingWriteHandler.WriteStreamInfo res = handler.readRecords(500);
		assertThat(res.getRecords()).as("Should read 1 record").hasSize(1);
		assertThat(res.getOperationType()).isEqualTo(UPSERT);
		res = handler.readRecords(500);
		assertThat(res.getRecords()).as("Should read 1 record").hasSize(1);
		assertThat(res.getOperationType()).isEqualTo(DELETE);
		res = handler.readRecords(500);
		assertThat(res.getRecords()).as("Should read 1 record").hasSize(1);
		assertThat(res.getOperationType()).isEqualTo(UPSERT);

	}
}
