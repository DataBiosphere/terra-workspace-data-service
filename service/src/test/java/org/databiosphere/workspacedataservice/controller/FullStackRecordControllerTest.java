package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedata.model.ErrorResponse;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;
import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;

/**
 * This test spins up a web server and the full Spring Boot web stack. It was
 * necessary to add it in order to test error handling since MockMvc doesn't
 * match full Spring Boot error handling:
 * https://github.com/spring-projects/spring-framework/issues/17290 As a result,
 * this test suite is currently focused on validating expected error handling
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FullStackRecordControllerTest {
	@Autowired
	private TestRestTemplate restTemplate;
	private static HttpHeaders headers;
	private static UUID instanceId;
	private static final String versionId = "v0.2";
	private final ObjectMapper mapper = new ObjectMapper();
	@BeforeAll
	static void setUp() {
		headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		instanceId = UUID.randomUUID();
	}

	@Test
	@Transactional
	void missingReferencedRecordTypeShouldFail() throws JsonProcessingException {
		Map<String, Object> attrs = new HashMap<>();
		attrs.put("attr_ref", RelationUtils.createRelationString("non_existent", "recordId"));
		attrs.put("attr_ref_2", RelationUtils.createRelationString("non_existent_2", "recordId"));
		HttpEntity<String> requestEntity = new HttpEntity<>(
				mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attrs))), headers);
		ResponseEntity<ErrorResponse> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT, requestEntity,
				ErrorResponse.class, instanceId, versionId, "samples-1", "sample_1");
		ErrorResponse err = response.getBody();
		assertThat(err.getMessage()).isEqualTo("Referenced table(s) [non_existent_2, non_existent] do(es) not exist");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
	}

	@Test
	@Transactional
	void referencingMissingRecordShouldFail() throws Exception {
		Map<String, Object> attrs = new HashMap<>();
		String referencedRecordType = "referenced-type";
		createSomeRecords(referencedRecordType, 1);
		attrs.put("attr_ref", RelationUtils.createRelationString(referencedRecordType, "missing-id"));
		HttpEntity<String> requestEntity = new HttpEntity<>(
				mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attrs))), headers);
		ResponseEntity<ErrorResponse> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT, requestEntity,
				ErrorResponse.class, instanceId, versionId, "samples-2", "sample_1");
		ErrorResponse responseContent = response.getBody();
		assertThat(responseContent.getMessage())
				.isEqualTo("It looks like you're trying to reference a record that does not exist.");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
	}

	@Test
	@Transactional
	void retrievingMissingEntityShouldFail() throws Exception {
		createSomeRecords("samples", 1);
		HttpEntity<String> requestEntity = new HttpEntity<>(headers);
		ResponseEntity<ErrorResponse> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.GET, requestEntity,
				ErrorResponse.class, instanceId, versionId, "samples", "sample_1");
		ErrorResponse responseContent = response.getBody();
		assertThat(responseContent.getMessage()).isEqualTo("Record not found");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}

	@Test
	void invalidApiVersionShouldFail() {
		ResponseEntity<LinkedHashMap> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.GET, new HttpEntity<>(headers),
				LinkedHashMap.class, instanceId, "garbage", "type", "id");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
		assertThat(response.getBody()).containsEntry("message", "Invalid API version specified");
	}

	private void createSomeRecords(String recordType, int numRecords) throws Exception {
		for (int i = 0; i < numRecords; i++) {
			String recordId = "record_" + i;
			Map<String, Object> attributes = generateRandomAttributes();
			ResponseEntity<String> response = restTemplate.exchange(
					"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT,
					new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))),
							headers),
					String.class, instanceId, versionId, recordType, recordId);
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
		}
	}

}
