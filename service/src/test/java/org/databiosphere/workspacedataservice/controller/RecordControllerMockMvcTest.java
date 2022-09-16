package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.service.RelationUtils;

import java.util.*;

import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.*;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
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
		RecordType recordType1 = RecordType.valueOf("recordType1");
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
		RecordType recordType1 = RecordType.valueOf("illegalName");
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
	void putNewRecord() throws Exception {
		String newRecordType = "newRecordType";
		RecordAttributes attributes = new RecordAttributes(Map.of("foo", "bar", "num", 123));
		// create new record with new record type
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				newRecordType, "newRecordId").content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());

		RecordAttributes attributes2 = new RecordAttributes(Map.of("foo", "baz", "num", 888));
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				newRecordType, "newRecordId2").content(mapper.writeValueAsString(new RecordRequest(attributes2)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());

		// now update the second new record
		RecordAttributes attributes3 = new RecordAttributes(Map.of("foo", "updated", "num", 999));
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				newRecordType, "newRecordId2").content(mapper.writeValueAsString(new RecordRequest(attributes3)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk());
	}
	@Test
	@Transactional
	void ensurePutShowsNewlyNullFields() throws Exception {
		RecordType recordType1 = RecordType.valueOf("recordType1");
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
		RecordType recordType1 = RecordType.valueOf("recordType1");
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
		RecordType recordType = RecordType.valueOf("samples");
		createSomeRecords(recordType, 1);
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")).andExpect(status().isOk()).andExpect(jsonPath("$.id", is("record_0")));
	}

	@Test
	@Transactional
	void createRecordWithReferences() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
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
		RecordType referencedType = RecordType.valueOf("missing");
		RecordType referringType = RecordType.valueOf("ref_samples-2");
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
		RecordType referencedType = RecordType.valueOf("ref_participants-2");
		RecordType referringType = RecordType.valueOf("ref_samples-3");
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
		RecordType recordType = RecordType.valueOf("to-alter");
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String newTextValue = "convert this column from date to text";
		attributes.put("attr3", newTextValue);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_1").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isCreated()).andExpect(jsonPath("$.attributes.attr3", is(newTextValue)));
	}

	@Test
	@Transactional
	void patchMissingRecord() throws Exception {
		RecordType recordType = RecordType.valueOf("to-patch");
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
		String ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), "missing_also");
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
	void putRecordWithMismatchedReference() throws Exception {
		RecordType referencedType = RecordType.valueOf("referenced_Type");
		RecordType referringType = RecordType.valueOf("referring_Type");
		String recordId = "record_0";
		createSomeRecords(referencedType, 1);
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, recordId);
		attributes.put("ref-attr", ref);
		// Add referencing attribute to referring_Type
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, recordId)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk());
		// Create a new referring_Type that puts a reference to a non-existent
		// recordType in the pre-existing referencing attribute
		Map<String, Object> new_attributes = new HashMap<>();
		String invalid_ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), recordId);
		new_attributes.put("ref-attr", invalid_ref);

		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "new_record")
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(new_attributes))))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isBadRequest());
	}

	@Test
	@Transactional
	void tryToAssignReferenceToNonRefColumn() throws Exception {
		RecordType recordType = RecordType.valueOf("ref-alter");
		createSomeRecords(recordType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), "missing_also");
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
		RecordType recordType = RecordType.valueOf("samples");
		createSomeRecords(recordType, 1);
		mockMvc.perform(delete("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")).andExpect(status().isNoContent());
		mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "missing-2")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void deleteMissingRecord() throws Exception {
		RecordType recordType = RecordType.valueOf("samples");
		createSomeRecords(recordType, 1);
		mockMvc.perform(delete("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_1")).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void deleteReferencedRecord() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
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

	@Test
	@Transactional
	void deleteRecordType() throws Exception {
		String recordType = "recordType";
		createSomeRecords(recordType, 3);
		mockMvc.perform(delete("/{instanceId}/types/{v}/{type}", instanceId, versionId, recordType))
				.andExpect(status().isNoContent());
		mockMvc.perform(get("/{instanceId}/types/{version}/{type}", instanceId, versionId, recordType))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void deleteNonExistentRecordType() throws Exception {
		String recordType = "recordType";
		mockMvc.perform(delete("/{instanceId}/types/{version}/{recordType}", instanceId, versionId, recordType))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void deleteReferencedRecordType() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		createSomeRecords(referencedType, 3);
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("sample-ref", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk()).andExpect(content().string(containsString(ref)));

		mockMvc.perform(delete("/{instanceId}/types/{version}/{recordType}", instanceId, versionId, referencedType))
				.andExpect(status().isConflict());
	}

	@Test
	@Transactional
	void deleteReferencedRecordTypeWithNoRecords() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		createSomeRecords(referencedType, 3);
		createSomeRecords(referringType, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("sample-ref", ref);
		// Create relation column
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk()).andExpect(content().string(containsString(ref)));

		// Delete record from referencing type
		mockMvc.perform(delete("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0")).andExpect(status().isNoContent());

		// Attempt to delete referenced type
		mockMvc.perform(delete("/{instanceId}/types/{version}/{recordType}", instanceId, versionId, referencedType))
				.andExpect(status().isConflict());
	}

	@Test
	@Transactional
	void describeType() throws Exception {
		RecordType type = RecordType.valueOf("recordType");
		createSomeRecords(type, 1);

		RecordType referencedType = RecordType.valueOf("referencedType");
		createSomeRecords(referencedType, 1);
		createSomeRecords(type, 1);
		Map<String, Object> attributes = new HashMap<>();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.put("attr-ref", ref);

		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId, type,
				"record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes)))))
				.andExpect(status().isOk()).andExpect(content().string(containsString(ref)));

		List<AttributeSchema> expectedAttributes = Arrays.asList(new AttributeSchema("attr-boolean", "BOOLEAN", null),
				new AttributeSchema("attr-dt", "DATE_TIME", null), new AttributeSchema("attr-json", "JSON", null),
				new AttributeSchema("attr-ref", "RELATION", referencedType),
				new AttributeSchema("attr1", "STRING", null), new AttributeSchema("attr2", "DOUBLE", null),
				new AttributeSchema("attr3", "DATE", null), new AttributeSchema("attr4", "STRING", null),
				new AttributeSchema("attr5", "LONG", null));

		RecordTypeSchema expected = new RecordTypeSchema(type, expectedAttributes, 1);

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
				.andExpect(status().isOk()).andReturn();

		RecordTypeSchema actual = mapper.readValue(mvcResult.getResponse().getContentAsString(),
				RecordTypeSchema.class);

		assertEquals(expected, actual);
	}

	@Test
	@Transactional
	void describeNonexistentType() throws Exception {
		mockMvc.perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, "noType"))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void describeAllTypes() throws Exception {
		// replace instanceId for this test so only these records are found
		UUID instId = UUID.randomUUID();
		RecordType type1 = RecordType.valueOf("firstType");
		createSomeRecords(type1, 1, instId);
		RecordType type2 = RecordType.valueOf("secondType");
		createSomeRecords(type2, 2, instId);
		RecordType type3 = RecordType.valueOf("thirdType");
		createSomeRecords(type3, 10, instId);

		List<AttributeSchema> expectedAttributes = Arrays.asList(new AttributeSchema("attr-boolean", "BOOLEAN", null),
				new AttributeSchema("attr-dt", "DATE_TIME", null), new AttributeSchema("attr-json", "JSON", null),
				new AttributeSchema("attr1", "STRING", null), new AttributeSchema("attr2", "DOUBLE", null),
				new AttributeSchema("attr3", "DATE", null), new AttributeSchema("attr4", "STRING", null),
				new AttributeSchema("attr5", "LONG", null));

		List<RecordTypeSchema> expectedSchemas = Arrays.asList(new RecordTypeSchema(type1, expectedAttributes, 1),
				new RecordTypeSchema(type2, expectedAttributes, 2),
				new RecordTypeSchema(type3, expectedAttributes, 10));

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}", instId, versionId))
				.andExpect(status().isOk()).andReturn();

		List<RecordTypeSchema> actual = Arrays
				.asList(mapper.readValue(mvcResult.getResponse().getContentAsString(), RecordTypeSchema[].class));

		assertEquals(expectedSchemas, actual);
	}

	@Test
	@Transactional
	void describeAllTypesNoInstance() throws Exception {
		mockMvc.perform(get("/{instanceId}/types/{v}", UUID.randomUUID(), versionId)).andExpect(status().isNotFound());
	}

	private void createSomeRecords(String recordType, int numRecords) throws Exception {
		createSomeRecords(RecordType.valueOf(recordType), numRecords, instanceId);
	}

	private void createSomeRecords(RecordType recordType, int numRecords) throws Exception {
		createSomeRecords(recordType, numRecords, instanceId);
	}

	private void createSomeRecords(String recordType, int numRecords, UUID instId) throws Exception {
		createSomeRecords(RecordType.valueOf(recordType), numRecords, instId);
	}

	private void createSomeRecords(RecordType recordType, int numRecords, UUID instId) throws Exception {
		for (int i = 0; i < numRecords; i++) {
			String recordId = "record_" + i;
			Map<String, Object> attributes = generateRandomAttributes();
			mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instId, versionId,
					recordType, recordId)
							.content(mapper.writeValueAsString(new RecordRequest(new RecordAttributes(attributes))))
							.contentType(MediaType.APPLICATION_JSON))
					.andExpect(status().is2xxSuccessful());
		}
	}
}
