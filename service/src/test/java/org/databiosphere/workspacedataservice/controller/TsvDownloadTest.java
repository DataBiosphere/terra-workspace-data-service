package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TsvDownloadTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private RecordController recordController;
	private String version;
	private UUID instanceId;

	@Autowired
    private ObjectReader tsvReader;

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
		InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(reader);
        RecordAttributes recordAttributes = tsvIterator.next();
		assertThat(recordAttributes.getAttributeValue(primaryKeyName)).isEqualTo(primaryKeyName+"_val");
		assertThat(recordAttributes.getAttributeValue("col_1")).isEqualTo("Fido");
		assertThat(recordAttributes.getAttributeValue("col_2")).isEqualTo("Jerry");
		assertThat(tsvIterator.hasNext()).isFalse();
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
		InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(reader);
		RecordAttributes recordAttributes = tsvIterator.next();
		assertThat(recordAttributes.getAttributeValue("description")).isEqualTo("Embedded\tTab");
		recordAttributes = tsvIterator.next();
		assertThat(recordAttributes.getAttributeValue("description")).isEqualTo("\n,Weird\n String");
		assertThat(recordAttributes.getAttributeValue("location")).isEqualTo("Cambridge, \"MA\"");
		assertThat(recordAttributes.getAttributeValue("unicodeData")).isEqualTo("\uD83D\uDCA9ȇ");
		assertThat(tsvIterator.hasNext()).isFalse();
		reader.close();
	}
}
