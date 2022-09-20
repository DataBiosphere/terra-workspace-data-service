package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedata.model.ErrorResponse;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
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
	void testBadRecordTypeNames() throws JsonProcessingException {
		HttpEntity<String> requestEntity = new HttpEntity<>(
				mapper.writeValueAsString(new RecordRequest(RecordAttributes.empty())), headers);
		List<String> badNames = List.of("); drop table users;", "$$foo.bar", "...", "&Q$(*^@$(*");
		for (String badName : badNames) {
			ResponseEntity<ErrorResponse> response = restTemplate.exchange(
					"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT, requestEntity,
					ErrorResponse.class, instanceId, versionId, badName, "sample_1");
			ErrorResponse err = response.getBody();
			assertThat(err.getMessage()).containsPattern("Record Type .* or contain characters besides letters");
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
		}
	}

	@Test
	@Transactional
	void testQuerySuccess() throws Exception {
		RecordType recordType = RecordType.valueOf("for_query");
		List<String> names = List.of("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
				"Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");
		Iterator<String> namesIterator = names.iterator();
		createSomeRecords(recordType, 26, namesIterator::next);
		int limit = 5;
		int offset = 0;
		RecordQueryResponse body = executeQuery(recordType, RecordQueryResponse.class).getBody();
		assertThat(body.records()).as("When no search request POST body is sent, we should use defaults").hasSize(10);
		body = executeQuery(recordType, RecordQueryResponse.class, new SearchRequest(limit, offset, SortDirection.ASC))
				.getBody();
		assertThat(body.records()).hasSize(limit);
		assertThat(body.records().get(0).recordId())
				.as("A should be the first record id in ascending order").isEqualTo("A");
		assertThat(body.records().get(4).recordId()).isEqualTo("E");
		body = executeQuery(recordType, RecordQueryResponse.class, new SearchRequest(limit, offset, SortDirection.DESC))
				.getBody();
		assertThat(body.records()).hasSize(limit);
		assertThat(body.records().get(0).recordId())
				.as("Z should be first record id in descending order").isEqualTo("Z");
		assertThat(body.records().get(4).recordId()).isEqualTo("V");
		offset = 10;
		body = executeQuery(recordType, RecordQueryResponse.class, new SearchRequest(limit, offset, SortDirection.ASC))
				.getBody();
		assertThat(body.records().get(0).recordId())
				.as("K should be first record id in ascending order with offset of 10").isEqualTo("K");
	}

	@Test
	@Transactional
	void testQueryFailures() throws Exception {
		RecordType recordType = RecordType.valueOf("for_query");
		int limit = 5;
		int offset = 0;
		ResponseEntity<ErrorResponse> response = executeQuery(recordType, ErrorResponse.class,
				new SearchRequest(limit, offset, SortDirection.ASC));
		assertThat(response.getStatusCode()).as("record type doesn't exist").isEqualTo(HttpStatus.NOT_FOUND);
		createSomeRecords(recordType, 1);
		limit = 1001;
		response = executeQuery(recordType, ErrorResponse.class, new SearchRequest(limit, offset, SortDirection.ASC));
		assertThat(response.getStatusCode()).as("unsupported limit size").isEqualTo(HttpStatus.BAD_REQUEST);
		limit = 0;
		response = executeQuery(recordType, ErrorResponse.class, new SearchRequest(limit, offset, SortDirection.ASC));
		assertThat(response.getStatusCode()).as("unsupported limit size").isEqualTo(HttpStatus.BAD_REQUEST);
	}

	private <T> ResponseEntity<T> executeQuery(RecordType recordType, Class<T> responseType, SearchRequest... request)
			throws JsonProcessingException {
		HttpEntity<String> requestEntity = new HttpEntity<>(
				request != null && request.length > 0 ? mapper.writeValueAsString(request[0]) : "", headers);
		return restTemplate.exchange("/{instanceid}/search/{v}/{type}", HttpMethod.POST, requestEntity, responseType,
				instanceId, versionId, recordType);
	}

	@Test
	@Transactional
	void testBadAttributeNames() throws JsonProcessingException {
		List<String> badNames = List.of("create table buttheads(id int)", "samples\n11", "##magic beans!");
		for (String badName : badNames) {
			RecordAttributes attributes = RecordAttributes.empty();
			attributes.put(badName, "foo");
			HttpEntity<String> requestEntity = new HttpEntity<>(
					mapper.writeValueAsString(new RecordRequest(attributes)), headers);
			ResponseEntity<ErrorResponse> response = restTemplate.exchange(
					"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT, requestEntity,
					ErrorResponse.class, instanceId, versionId, "sample", "sample_1");
			ErrorResponse err = response.getBody();
			assertThat(err.getMessage()).containsPattern("Attribute .* or contain characters besides letters");
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
		}
	}

	@Test
	@Transactional
	void missingReferencedRecordTypeShouldFail() throws JsonProcessingException {
		RecordAttributes attrs = RecordAttributes.empty();
		attrs.put("attr_ref", RelationUtils.createRelationString(RecordType.valueOf("non_existent"), "recordId"));
		attrs.put("attr_ref_2", RelationUtils.createRelationString(RecordType.valueOf("non_existent_2"), "recordId"));
		HttpEntity<String> requestEntity = new HttpEntity<>(
				mapper.writeValueAsString(new RecordRequest(attrs)), headers);
		ResponseEntity<ErrorResponse> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT, requestEntity,
				ErrorResponse.class, instanceId, versionId, "samples-1", "sample_1");
		ErrorResponse err = response.getBody();
		assertThat(err.getMessage()).isEqualTo("Record type for relation does not exist");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}

	@Test
	@Transactional
	void referencingMissingRecordShouldFail() throws Exception {
		RecordAttributes attrs = RecordAttributes.empty();
		RecordType referencedRecordType = RecordType.valueOf("referenced-type");
		createSomeRecords(referencedRecordType, 1);
		attrs.put("attr_ref", RelationUtils.createRelationString(referencedRecordType, "missing-id"));
		HttpEntity<String> requestEntity = new HttpEntity<>(
				mapper.writeValueAsString(new RecordRequest(attrs)), headers);
		ResponseEntity<ErrorResponse> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT, requestEntity,
				ErrorResponse.class, instanceId, versionId, "samples-2", "sample_1");
		ErrorResponse responseContent = response.getBody();
		assertThat(responseContent.getMessage())
				.isEqualTo("It looks like you're trying to reference a record that does not exist.");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
	}

	@Test
	@Transactional
	void retrievingMissingEntityShouldFail() throws Exception {
		createSomeRecords(RecordType.valueOf("samples"), 1);
		HttpEntity<String> requestEntity = new HttpEntity<>(headers);
		ResponseEntity<ErrorResponse> response = restTemplate.exchange(
				"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.GET, requestEntity,
				ErrorResponse.class, instanceId, versionId, "samples", "sample_1");
		ErrorResponse responseContent = response.getBody();
		assertThat(responseContent.getMessage()).isEqualTo("Record does not exist");
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

	private void createSomeRecords(RecordType recordType, int numRecords, Supplier<String>... recordIdSupplier)
			throws Exception {
		for (int i = 0; i < numRecords; i++) {
			String recordId = recordIdSupplier == null || recordIdSupplier.length == 0
					? "record_" + i
					: recordIdSupplier[0].get();
			RecordAttributes attributes = generateRandomAttributes();
			ResponseEntity<String> response = restTemplate.exchange(
					"/{instanceId}/records/{version}/{recordType}/{recordId}", HttpMethod.PUT,
					new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(attributes)),
							headers),
					String.class, instanceId, versionId, recordType, recordId);
			assertThat(response.getStatusCode()).isIn(HttpStatus.CREATED, HttpStatus.OK);
		}
	}

}
