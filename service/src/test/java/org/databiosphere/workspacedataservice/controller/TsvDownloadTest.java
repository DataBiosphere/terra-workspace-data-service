package org.databiosphere.workspacedataservice.controller;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.servlet.function.ServerRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TsvDownloadTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private RecordController recordController;
	private String version;
	private UUID instanceId;


	@BeforeEach
	void init(){
		version = "v0.2";
		instanceId = UUID.randomUUID();
		recordController.createInstance(instanceId, version);
	}

	@AfterEach
	void afterEach() {
		recordController.deleteInstance(instanceId, version);
	}

	@ParameterizedTest(name = "PK name {0} should be honored")
	@ValueSource(strings = {"Alfalfa", "buckWheat", "boo-yah", "sample id", "sample_id", "buttHead", "may 12 sample"})
	void tsvUploadWithChoosenPrimaryKeyFollowedByDownload(String primaryKeyName) throws IOException {
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				("col_1\tcol_2\t" + primaryKeyName + "\n" + "Fido\tJerry\t" + primaryKeyName + "_val\n").getBytes());
		String recordType = primaryKeyName+"_rt";
		recordController.tsvUpload(instanceId, version, RecordType.valueOf(recordType), Optional.of(primaryKeyName), file);
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
		assertThat(rcd.get(primaryKeyName)).isEqualTo(primaryKeyName+"_val");
		assertThat(rcd.get("col_1")).isEqualTo("Fido");
		assertThat(rcd.get("col_2")).isEqualTo("Jerry");
		assertThat(iterator.hasNext()).isFalse();
		reader.close();
	}
	@Test
	void batchWriteFollowedByTsvDownload() throws IOException {
		RecordType recordType = RecordType.valueOf("tsv-test");

		InputStream is = TsvDownloadTest.class.getResourceAsStream("/batch_write_tsv_data.json");
		ResponseEntity<BatchResponse> response = recordController.streamingWrite(instanceId, version, recordType, Optional.empty(), is);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getBody().recordsModified()).isEqualTo(2);
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
