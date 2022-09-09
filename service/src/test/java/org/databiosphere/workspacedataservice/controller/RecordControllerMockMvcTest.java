package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.exception.*;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
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
	void createInstanceAndTryToCreateAgain() throws Exception {
		UUID uuid = UUID.randomUUID();
		mockMvc.perform(post("/{instanceId}/{version}/", uuid, versionId)).andExpect(status().isCreated());
		mockMvc.perform(post("/{instanceId}/{version}/", uuid, versionId)).andExpect(status().isConflict());
	}

	@Test
	@Transactional
	void tryFetchingMissingRecordType() throws Exception {
		mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				"missing", "missing-2")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void tryFetchingMissingRecord() throws Exception {
		String recordType1 = "recordType1";
		createSomeRecords(recordType1, 1);
		mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "missing-2")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void tryCreatingIllegallyNamedRecordType() throws Exception {
		String recordType = "sys_my_type";
		Map<String, Object> attributes = new HashMap<>();
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "recordId")
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isBadRequest())
				.andExpect(result -> assertTrue(result.getResolvedException() instanceof InvalidNameException));
	}

	@Test
	@Transactional
	void updateWithIllegalAttributeName() throws Exception {
		String recordType1 = "illegalName";
		createSomeRecords(recordType1, 1);
		Map<String, Object> illegalAttribute = new HashMap<>();
		illegalAttribute.put("sys_foo", "some_val");
		mockMvc.perform(patch("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(illegalAttribute)))))
				.andExpect(status().isBadRequest())
				.andExpect(result -> assertTrue(result.getResolvedException() instanceof InvalidNameException));
	}
	@Test
	@Transactional
	void ensurePutShowsNewlyNullFields() throws Exception {
		String recordType1 = "recordType1";
		createSomeRecords(recordType1, 1);
		Map<String, Object> newAttributes = new HashMap<>();
		newAttributes.put("new-attr", "some_val");
		mockMvc.perform(put("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(newAttributes)))))
				.andExpect(content().string(containsString("\"attr3\":null")))
				.andExpect(content().string(containsString("\"attr-dt\":null"))).andExpect(status().isOk());
	}

	@Test
	void ensurePatchShowsAllFields() throws Exception {
		String recordType1 = "recordType1";
		createSomeRecords(recordType1, 1);
		Map<String, Object> newAttributes = new HashMap<>();
		newAttributes.put("new-attr", "some_val");
		mockMvc.perform(patch("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(newAttributes)))))
				.andExpect(status().isOk()).andExpect(jsonPath("$.attributes.new-attr", is("some_val")));
	}

	@Test
	@Transactional
	void createAndRetrieveRecord() throws Exception {
		String recordType = "samples";
		createSomeRecords(recordType, 1);
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")).andExpect(status().isOk()).andExpect(jsonPath("$.id", is("record_0")));
	}

	@Test
	@Transactional
	void createRecordWithReferences() throws Exception {
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
				.andExpect(status().isOk()).andExpect(jsonPath("$.attributes.sample-ref", is(ref)));
	}

	@Test
	@Transactional
	void referencingMissingTableFails() throws Exception {
		String referencedType = "missing";
		String referringType = "ref_samples-2";
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("sample-ref", ref);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isNotFound())
				.andExpect(result -> assertTrue(result.getResolvedException() instanceof MissingObjectException));
	}

	@Test
	@Transactional
	void referencingMissingRecordFails() throws Exception {
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
				.andExpect(status().isForbidden())
				.andExpect(result -> assertTrue(result.getResolvedException() instanceof InvalidRelationException));
	}

	@Test
	@Transactional
	void expandColumnDefForNewData() throws Exception {
		String recordType = "to-alter";
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String newTextValue = "convert this column from date to text";
		attributes.put("attr3", newTextValue);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_1").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk()).andExpect(jsonPath("$.attributes.attr3", is(newTextValue)));
	}

	@Test
	@Transactional
	void patchMissingRecord() throws Exception {
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
	void putRecordWithMissingTableReference() throws Exception {
		String recordType = "record-type-missing-table-ref";
		String recordId = "record_0";
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString("missing", "missing_also");
		attributes.put("sample-ref", ref);

		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, recordId)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isNotFound())
				.andExpect(result -> assertTrue(result.getResolvedException() instanceof MissingObjectException));
	}

	@Test
	@Transactional
	void tryToAssignReferenceToNonRefColumn() throws Exception {
		String recordType = "ref-alter";
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString("missing", "missing_also");
		attributes.put("attr1", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isForbidden())
				.andExpect(result -> assertTrue(result.getResolvedException().getMessage()
						.contains("relation to an existing attribute that was not configured for relations")));
	}

	@Test
	@Transactional
	void deleteRecord() throws Exception {
		String recordType = "samples";
		createSomeRecords(recordType, 1);
		mockMvc.perform(delete("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")).andExpect(status().isNoContent());
		mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "missing-2")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void deleteMissingRecord() throws Exception {
		String recordType = "samples";
		createSomeRecords(recordType, 1);
		mockMvc.perform(delete("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_1")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void deleteReferencedRecord() throws Exception {
		String referencedType = "ref_participants";
		String referringType = "ref_samples";
		createSomeRecords(referencedType, 1);
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("sample-ref", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk()).andExpect(content().string(containsString(ref)));
		mockMvc.perform(delete("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referencedType, "record_0")).andExpect(status().isBadRequest());
	}

	private void createSomeRecords(String recordType, int numRecords) throws Exception {
		for (int i = 0; i < numRecords; i++) {
			String recordId = "record_" + i;
			Map<String, Object> attributes = generateRandomAttributes();
			mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
					recordType, recordId)
							.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
							.contentType(MediaType.APPLICATION_JSON))
					.andExpect(status().is2xxSuccessful());
		}
	}
}
