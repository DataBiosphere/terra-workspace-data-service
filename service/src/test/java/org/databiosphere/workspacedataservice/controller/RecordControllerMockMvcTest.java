package org.databiosphere.workspacedataservice.controller;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordId;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@AutoConfigureMockMvc
public class RecordControllerMockMvcTest {

	private final ObjectMapper mapper = new ObjectMapper();
	@Autowired
	private MockMvc mockMvc;

	private static UUID instanceId;

	private static String versionId = "v0.2";

	@BeforeAll
	private static void createWorkspace() {
		instanceId = UUID.randomUUID();
	}

	@Test
	@Transactional
	public void createInstanceAndTryToCreateAgain() throws Exception {
		UUID uuid = UUID.randomUUID();
		mockMvc.perform(post("/{instanceId}/{version}/", uuid, versionId)).andExpect(status().isCreated());
		mockMvc.perform(post("/{instanceId}/{version}/", uuid, versionId)).andExpect(status().isConflict());
	}

	@Test
	@Transactional
	public void tryFetchingMissingRecordType() throws Exception {
		mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				"missing", "missing-2")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	public void tryFetchingMissingRecord() throws Exception {
		String recordType1 = "recordType1";
		createSomeRecords(recordType1, 1);
		mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "missing-2")).andExpect(status().isNotFound());
	}

	@Test
	public void ensurePutShowsNewlyNullFields() throws Exception {
		String recordType1 = "recordType1";
		createSomeRecords(recordType1, 1);
		Map<String, Object> newAttributes = new HashMap<>();
		newAttributes.put("new-attr", "some_val");
		mockMvc.perform(put("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0")
				.contentType(MediaType.APPLICATION_JSON)
				.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(newAttributes)))))
				.andExpect(content().string(containsString("\"attr3\":null")))
				.andExpect(content().string(containsString("\"attr-dt\":null")))
				.andExpect(status().isOk());
	}

	@Test
	public void ensurePatchShowsAllFields() throws Exception {
		String recordType1 = "recordType1";
		createSomeRecords(recordType1, 1);
		Map<String, Object> newAttributes = new HashMap<>();
		newAttributes.put("new-attr", "some_val");
		mockMvc.perform(patch("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
						recordType1, "record_0")
						.contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(newAttributes)))))
				.andExpect(content().string(containsString("\"attr3\"")))
				.andExpect(content().string(containsString("\"attr-dt\"")))
				.andExpect(content().string(containsString("\"new-attr\":\"some_val\"")))
				.andExpect(status().isOk());
	}

	@Test
	@Transactional
	public void createAndRetrieveRecord() throws Exception {
		String recordType = "samples";
		createSomeRecords(recordType, 1);
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")).andExpect(status().isOk());
	}

	@Test
	@Transactional
	public void createRecordWithReferences() throws Exception {
		String referencedType = "ref_participants";
		String referringType = "ref_samples";
		createSomeRecords(referencedType, 3);
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("sample-ref", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk()).andExpect(content().string(containsString(ref)));
	}

	@Test
	@Transactional
	public void referencingMissingTableFails() throws Exception {
		String referencedType = "missing";
		String referringType = "ref_samples-2";
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("sample-ref", ref);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isBadRequest()).andExpect(content().string(containsString(
						"It looks like you're attempting to assign a relation to a table, missing, that does not exist")));;
	}

	@Test
	@Transactional
	public void referencingMissingRecordFails() throws Exception {
		String referencedType = "ref_participants-2";
		String referringType = "ref_samples-3";
		createSomeRecords(referencedType, 3);
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_99");
		attributes.put("sample-ref", ref);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isBadRequest()).andExpect(content().string(
						containsString("It looks like you're trying to reference a record that does not exist.")));
	}

	@Test
	@Transactional
	public void expandColumnDefForNewData() throws Exception {
		String recordType = "to-alter";
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		attributes.put("attr3", "convert this column from date to text");
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_1").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk());
	}

	@Test
	@Transactional
	public void patchMissingRecord() throws Exception {
		String recordType = "to-patch";
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		attributes.put("attr-boolean", true);
		String recordId = "record_missing";
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, recordId)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	public void putRecordWithMissingTableReference() throws Exception {
		String recordType = "record-type-missing-table-ref";
		String recordId = "record_0";
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString("missing", "missing_also");
		attributes.put("sample-ref", ref);

		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, recordId)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isBadRequest())
				.andExpect(content().string(containsString("assign a relation to a table that does not exist")));
	}

	@Test
	@Transactional
	public void tryToAssignReferenceToNonRefColumn() throws Exception {
		String recordType = "ref-alter";
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString("missing", "missing_also");
		attributes.put("attr1", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isConflict())
				.andExpect(result -> assertTrue(result.getResolvedException().getMessage()
						.contains("relation to an existing column that was not configured for relations")));
	}

	private void createSomeRecords(String recordType, int numRecords) throws Exception {
		for (int i = 0; i < numRecords; i++) {
			String recordId = "record_" + i;
			Map<String, Object> attributes = new HashMap<>();
			attributes.put("attr1", RandomStringUtils.randomAlphabetic(6));
			attributes.put("attr2", RandomUtils.nextFloat());
			attributes.put("attr3", "2022-11-01");
			attributes.put("attr4", RandomStringUtils.randomNumeric(5));
			attributes.put("attr5", RandomUtils.nextLong());
			attributes.put("attr-dt", "2022-03-01T12:00:03");
			attributes.put("attr-json", "{\"foo\":\"bar\"}");
			attributes.put("attr-boolean", true);
			mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
					recordType, recordId)
							.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
							.contentType(MediaType.APPLICATION_JSON))
					.andExpect(status().is2xxSuccessful());
		}
	}
}
