package org.databiosphere.workspacedataservice.controller;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TsvDownloadTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private RecordController recordController;

	@Test
	void batchWriteFollowedByTsvDownload() throws IOException {
		RecordType recordType = RecordType.valueOf("tsv-test");
		String version = "v0.2";
		UUID instanceId = UUID.randomUUID();
		recordController.createInstance(instanceId, version);
		InputStream is = TsvDownloadTest.class.getResourceAsStream("/batch_write_tsv_data.json");
		ResponseEntity<BatchResponse> response = recordController.streamingWrite(instanceId, version, recordType, is);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		HttpHeaders headers = new HttpHeaders();
		ResponseEntity<Resource> stream = restTemplate.exchange("/{instanceId}/tsv/{version}/{recordType}",
				HttpMethod.GET, new HttpEntity<>(headers), Resource.class, instanceId, version, recordType);
		InputStream inputStream = stream.getBody().getInputStream();
		CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setDelimiter("\t")
				.setQuoteMode(QuoteMode.MINIMAL).build();
		InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
		Iterable<CSVRecord> records = new CSVParser(reader, csvFormat);
		Iterator<CSVRecord> iterator = records.iterator();
		CSVRecord rcd = iterator.next();
		assertThat(rcd.get("description")).isEqualTo("Embedded\tTab");
		rcd = iterator.next();
		assertThat(rcd.get("description")).isEqualTo("\n,Weird\n String");
		assertThat(rcd.get("location")).isEqualTo("Cambridge, \"MA\"");
		assertThat(rcd.get("unicodeData")).isEqualTo("\uD83D\uDCA9\u0207");
		assertThat(iterator.hasNext()).isFalse();
		reader.close();
	}
}
