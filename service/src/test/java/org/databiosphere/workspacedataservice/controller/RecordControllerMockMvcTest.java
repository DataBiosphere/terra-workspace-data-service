package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.BatchOperation;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class RecordControllerMockMvcTest {
	@Autowired
	private ObjectMapper mapper;
	@Autowired
	private MockMvc mockMvc;

	private static UUID instanceId;

	private static String versionId = "v0.2";

	@BeforeEach
	void beforeEach() throws Exception {
		instanceId = UUID.randomUUID();
		mockMvc.perform(post("/instances/{v}/{instanceid}",
				versionId, instanceId).content("")).andExpect(status().isCreated());
	}

	@AfterEach
	void afterEach() throws Exception {
		try {
			mockMvc.perform(delete("/instances/{v}/{instanceid}",
					versionId, instanceId).content("")).andExpect(status().isOk());
		} catch (Throwable t)  {
			 // noop - if we fail to delete the instance, don't fail the test
		}
	}

	@Test
	@Transactional
	void createInstanceAndTryToCreateAgain() throws Exception {
		UUID uuid = UUID.randomUUID();
		mockMvc.perform(post("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isCreated());
		mockMvc.perform(post("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isConflict());
	}

	@Test
	@Transactional
	void deleteInstance() throws Exception {
		UUID uuid = UUID.randomUUID();
		// delete nonexistent instance should 404
		mockMvc.perform(delete("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isNotFound());
		// creating the instance should 201
		mockMvc.perform(post("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isCreated());
		// delete existing instance should 200
		mockMvc.perform(delete("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isOk());
		// deleting again should 404
		mockMvc.perform(delete("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isNotFound());
		// creating again should 201
		mockMvc.perform(post("/instances/{version}/{instanceId}", versionId, uuid)).andExpect(status().isCreated());
	}

	@Test
	@Transactional
	void deleteInstanceContainingData() throws Exception {
		RecordAttributes attributes = new RecordAttributes(Map.of("foo", "bar", "num", 123));
		// create "to" record, which will be the target of a relation
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						"to", "1").content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		// create "from" record, with a relation to "to"
		RecordAttributes attributes2 = new RecordAttributes(Map.of("relation", "terra-wds:/to/1"));
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						"from", "2").content(mapper.writeValueAsString(new RecordRequest(attributes2)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		// delete existing instance should 200
		mockMvc.perform(delete("/instances/{version}/{instanceId}", versionId, instanceId)).andExpect(status().isOk());
	}

	@Test
	@Transactional
	void tsvWithNoRowsShouldReturn400() throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "no_data.tsv", MediaType.TEXT_PLAIN_VALUE,
				"col1\tcol2\n".getBytes());

		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, "tsv-record-type")
				.file(file)).andExpect(status().isBadRequest());
	}

	@Test
	@Transactional
	void storeLargeIntegerValue() throws Exception {
		StringBuilder tsvContent = new StringBuilder("sys_name\tbigint\tbigfloat\n");
		String bigIntValue = "11111111111111111111111111111111";
		String bigFloatValue = "11111111111111111111111111111111.2222222222";
		tsvContent.append(1 + "\t" + bigIntValue + "\t" + bigFloatValue +"\n");
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				tsvContent.toString().getBytes());

		String recordType = "big-int-value";
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
				.file(file)).andExpect(status().isOk());
		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "1")).andReturn();
		RecordResponse recordResponse = mapper.readValue(mvcResult.getResponse().getContentAsString(), RecordResponse.class);
		assertEquals(new BigInteger(bigIntValue), recordResponse.recordAttributes().getAttributeValue("bigint"));
		assertEquals(new BigDecimal(bigFloatValue), recordResponse.recordAttributes().getAttributeValue("bigfloat"));
	}

	@Test
	@Transactional
	void writeAndReadJson() throws Exception {
		String rt = "jsonb-type";
		RecordAttributes attributes = new RecordAttributes(Map.of("json-attr", Map.of("name", "Bella", "age_in_months", 8)));
		// create new record with new record type
		String rId = "newRecordId";
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						rt, rId).content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		MockHttpServletResponse res = mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				rt, rId)).andExpect(jsonPath("$.attributes.json-attr.age_in_months", is(8))).andReturn().getResponse();
		RecordResponse recordResponse = mapper.readValue(res.getContentAsString(), RecordResponse.class);
		Object attributeValue = recordResponse.recordAttributes().getAttributeValue("json-attr");
		assertTrue(attributeValue instanceof Map, "jsonb data should deserialize to a map, " +
				"before getting serialized to json in the final response");
	}

	@Test
	@Transactional
	void writeAndReadAllDataTypesJson() throws Exception {
		String rt = "all-types";
		RecordAttributes attributes = TestUtils.getAllTypesAttributesForJson();
//		assertEquals(attributes.attributeSet().size(), DataTypeMapping.values().length);
		String rId = "newRecordId";
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						rt, rId).content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		String jsonRes = mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				rt, rId)).andReturn().getResponse().getContentAsString();
		assertEquals(TestUtils.getExpectedAllAttributesJsonText(), jsonRes);
	}

	@Test
	@Transactional
	void writeAndReadAllDataTypesTsv() throws Exception {
		String rt = "all-types";
		String recordId = "newRecordId";
		RecordAttributes attributes = TestUtils.getAllTypesAttributesForTsv();
//		assertEquals(DataTypeMapping.values().length, attributes.attributeSet().size());
		String tsv = "sys_name\t"+attributes.attributeSet().stream().map(Map.Entry::getKey).collect(Collectors.joining("\t")) + "\n";
		tsv += recordId+"\t" + attributes.attributeSet().stream().map(e -> e.getValue().toString()).collect(Collectors.joining("\t")) + "\n";

		MockMultipartFile file = new MockMultipartFile("records", "generated.tsv", MediaType.TEXT_PLAIN_VALUE,
				tsv.getBytes());
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId,
				rt).file(file)).andExpect(status().isOk());
		String jsonRes = mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				rt, recordId)).andReturn().getResponse().getContentAsString();
		assertEquals(TestUtils.getExpectedAllAttributesJsonText(), jsonRes);
	}

	@Test
	@Transactional
	void tsvWithMissingRowIdentifierColumn() throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "no_row_id.tsv", MediaType.TEXT_PLAIN_VALUE,
				"col1\tcol2\nfoo\tbar\n".getBytes());

		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}?uniqueRowIdentifierColumn=missing_row_id", instanceId, versionId, "tsv-missing-rowid")
				.file(file)).andExpect(status().isBadRequest());
	}

	@Test
	@Transactional
	void tsvWithEmptyStringIdentifier() throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "empty_row_id.tsv", MediaType.TEXT_PLAIN_VALUE,
				"col1\tcol2\n\tbar\n".getBytes());

		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, "tsv-missing-rowid")
				.file(file)).andExpect(status().isBadRequest());
	}

	@Test
	@Transactional
	void tsvWithSpecifiedRowIdentifierColumn() throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "specified_id.tsv", MediaType.TEXT_PLAIN_VALUE,
				"col1\tcol2\nfoo\tbar\n".getBytes());

		String recordType = "tsv_specified_row_id";
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}?uniqueRowIdentifierColumn=col2", instanceId, versionId, recordType)
				.file(file)).andExpect(status().isOk());
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId, recordType, "bar"))
				.andExpect(status().isOk());
	}

	@Test
	@Transactional
	void simpleTsvUploadWithBatchingShouldSucceed(@Value("${twds.write.batch.size}") int batchSize) throws Exception {
		StringBuilder tsvContent = new StringBuilder("sys_name\tcol1\n");
		for (int i = 0; i < batchSize + 1; i++) {
			tsvContent.append(i + "\ttada" + i + "\n");
		}
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				tsvContent.toString().getBytes());

		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, "tsv_batching")
				.file(file)).andExpect(status().isOk());
	}

	@Test
	@Transactional
	void nullAndNonNullArraysShouldChooseProperType() throws Exception {
		StringBuilder tsvContent = new StringBuilder("sys_name\tarray\n");
		//empty string/nulls
		for (int i = 0; i < 10; i++) {
			tsvContent.append(i + "null\t" + "\n");
		}
		//empty array
		for (int i = 0; i < 10; i++) {
			tsvContent.append(i + "empty\t[]\n");
		}
		//array of long
		for (int i = 0; i < 10; i++) {
			tsvContent.append(i + "valid\t[12]\n");
		}
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				tsvContent.toString().getBytes());

		String type = "tsv-record-type";
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, type)
				.file(file)).andExpect(status().isOk());

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
				.andExpect(status().isOk()).andReturn();

		RecordTypeSchema actual = mapper.readValue(mvcResult.getResponse().getContentAsString(),
				RecordTypeSchema.class);
		assertEquals("ARRAY_OF_NUMBER", actual.attributes().get(0).datatype());

		//upload a second time, this time with array of double
		StringBuilder secondUpload = new StringBuilder("sys_name\tarray\n");
		//array of long
		for (int i = 0; i < 10; i++) {
			secondUpload.append(i + "valid\t[12.99]\n");
		}
		file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				secondUpload.toString().getBytes());
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, type)
				.file(file)).andExpect(status().isOk());
		mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
				.andExpect(status().isOk()).andReturn();

		actual = mapper.readValue(mvcResult.getResponse().getContentAsString(),
				RecordTypeSchema.class);
		assertEquals("ARRAY_OF_NUMBER", actual.attributes().get(0).datatype());
	}

	@Test
	@Transactional
	void tsvWithMissingRelationShouldFail() throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "simple_bad_relation.tsv", MediaType.TEXT_PLAIN_VALUE,
				("sys_name\trelation\na\t" + RelationUtils.createRelationString(RecordType.valueOf("missing"), "QQ")
						+ "\n").getBytes());

		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, "tsv-record-type")
				.file(file)).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void uploadTsvAndVerifySchema() throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "test.tsv", MediaType.TEXT_PLAIN_VALUE,
				RecordControllerMockMvcTest.class.getResourceAsStream("/small-test.tsv"));

		String recordType = "tsv-types";
		mockMvc.perform(
				multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType).file(file))
				.andExpect(status().isOk());
		MvcResult schemaResult = mockMvc
				.perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType)).andReturn();
		RecordTypeSchema schema = mapper.readValue(schemaResult.getResponse().getContentAsString(),
				RecordTypeSchema.class);
		assertEquals("date", schema.attributes().get(0).name());
		assertEquals("DATE", schema.attributes().get(0).datatype());
		assertEquals("NUMBER", schema.attributes().get(1).datatype());
		assertEquals("double", schema.attributes().get(1).name());
		assertEquals("json", schema.attributes().get(4).name());
		assertEquals("JSON", schema.attributes().get(4).datatype());
		assertEquals("NUMBER", schema.attributes().get(5).datatype());
		assertEquals("long", schema.attributes().get(5).name());
		assertEquals("z_array_of_string", schema.attributes().get(7).name());
		assertEquals("ARRAY_OF_STRING", schema.attributes().get(7).datatype());
		assertEquals("z_double_array", schema.attributes().get(8).name());
		assertEquals("ARRAY_OF_NUMBER", schema.attributes().get(8).datatype());
		assertEquals("z_long_array", schema.attributes().get(9).name());
		assertEquals("ARRAY_OF_NUMBER", schema.attributes().get(9).datatype());
		assertEquals("z_z_boolean_array", schema.attributes().get(10).name());
		assertEquals("ARRAY_OF_BOOLEAN", schema.attributes().get(10).datatype());
		assertEquals("zz_array_of_date", schema.attributes().get(11).name());
		assertEquals("ARRAY_OF_DATE", schema.attributes().get(11).datatype());
		assertEquals("zz_array_of_datetime", schema.attributes().get(12).name());
		assertEquals("ARRAY_OF_DATE_TIME", schema.attributes().get(12).datatype());
		MockMultipartFile alter = new MockMultipartFile("records", "change_json_to_text.tsv",
				MediaType.TEXT_PLAIN_VALUE, "sys_name\tjson\na\tfoo\n".getBytes());
		mockMvc.perform(
				multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType).file(alter))
				.andExpect(status().isOk());
		schemaResult = mockMvc.perform(get("/{instanceid}/types/{v}/{type}", instanceId, versionId, recordType))
				.andReturn();
		schema = mapper.readValue(schemaResult.getResponse().getContentAsString(), RecordTypeSchema.class);
		assertEquals("json", schema.attributes().get(4).name());
		// data type should downgrade to STRING
		assertEquals("STRING", schema.attributes().get(4).datatype());
		//make sure left most column (sys_name) is used as id
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId, recordType, "a")).andExpect(status().isOk());
	}

	@Test
	@Transactional
	void tryDeletingMissingRecordType() throws Exception {
		mockMvc.perform(delete("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId, "missing", "missing-also"))
				.andExpect(status().isNotFound());
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
		RecordAttributes attributes = RecordAttributes.empty();
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "recordId").content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isBadRequest()).andExpect(result -> assertTrue(
						result.getResolvedException() instanceof MethodArgumentTypeMismatchException));
	}

	@Test
	@Transactional
	void updateWithIllegalAttributeName() throws Exception {
		RecordType recordType1 = RecordType.valueOf("illegalName");
		createSomeRecords(recordType1, 1);
		RecordAttributes illegalAttribute = RecordAttributes.empty();
		illegalAttribute.putAttribute("sys_foo", "some_val");
		mockMvc.perform(patch("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(illegalAttribute))))
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
		RecordAttributes newAttributes = RecordAttributes.empty();
		newAttributes.putAttribute("new-attr", "some_val");
		mockMvc.perform(put("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(newAttributes))))
				.andExpect(content().string(containsString("\"attr3\":null")))
				.andExpect(content().string(containsString("\"attr-dt\":null"))).andExpect(status().isOk());
	}

	@Test
	@Transactional
	void ensurePatchShowsAllFields() throws Exception {
		RecordType recordType1 = RecordType.valueOf("recordType1");
		createSomeRecords(recordType1, 1);
		RecordAttributes newAttributes = RecordAttributes.empty();
		newAttributes.putAttribute("new-attr", "some_val");
		mockMvc.perform(patch("/{instanceId}/records/{versionId}/{recordType}/{recordId}", instanceId, versionId,
				recordType1, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(newAttributes))))
				.andExpect(status().isOk()).andExpect(jsonPath("$.attributes.new-attr", is("some_val")));
	}

	@Test
	@Transactional
	void createAndRetrieveRecord() throws Exception {
		RecordType recordType = RecordType.valueOf("samples");
		createSomeRecords(recordType, 1);
		MockHttpServletResponse res = mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0")).andExpect(status().isOk()).andReturn().getResponse();
		RecordResponse recordResponse = mapper.readValue(res.getContentAsString(), RecordResponse.class);
		assertEquals("record_0", recordResponse.recordId());
		assertEquals("[1776-07-04, 1999-12-31]", recordResponse.recordAttributes().getAttributeValue("array-of-date").toString());
		assertEquals("[2021-01-06T13:30:00, 1980-10-31T23:59:00]", recordResponse.recordAttributes().getAttributeValue("array-of-datetime").toString());
	}

	@Test
	@Transactional
	void createRecordWithReferences() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		createSomeRecords(referencedType, 3);
		createSomeRecords(referringType, 1);
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.putAttribute("sample-ref", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isOk()).andExpect(jsonPath("$.attributes.sample-ref", is(ref)));
	}

	@Test
	@Transactional
	void createRecordWithReferenceArray() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		createSomeRecords(referencedType, 3);
		RecordAttributes attributes = RecordAttributes.empty();
		List<String> relArr = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> RelationUtils.createRelationString(referencedType, "record_" + i)).collect(Collectors.toList());
		attributes.putAttribute("rel-arr", relArr);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isCreated()).andExpect(jsonPath("$.attributes.rel-arr", is(relArr)));
	}

	@Test
	@Transactional
	void createRecordWithReferenceArrayMissingTable() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		RecordAttributes attributes = RecordAttributes.empty();
		List<String> relArr = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> RelationUtils.createRelationString(referencedType, "record_" + i)).collect(Collectors.toList());
		attributes.putAttribute("rel-arr", relArr);

		//Expect failure if relation table doesn't exist
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void createRecordWithReferenceArrayMissingRecord() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		RecordAttributes attributes = RecordAttributes.empty();
		List<String> relArr = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> RelationUtils.createRelationString(referencedType, "record_" + i)).collect(Collectors.toList());
		attributes.putAttribute("rel-arr", relArr);
		createSomeRecords(referencedType, 2);
		//Expect failure if only one relation is missing
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isForbidden());
	}

	@Test
	@Transactional
	void createRecordWithMixedReferenceArray() throws Exception {
		RecordType referencedType = RecordType.valueOf("ref_participants");
		RecordType referringType = RecordType.valueOf("ref_samples");
		RecordAttributes attributes = RecordAttributes.empty();
		List<String> relArr = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> RelationUtils.createRelationString(referencedType, "record_" + i)).collect(Collectors.toList());
		attributes.putAttribute("rel-arr", relArr);
		createSomeRecords(referencedType, 2);
//		//Expect failure if one relation refers to a different table
		relArr.set(2, RelationUtils.createRelationString(RecordType.valueOf("nonExistentType"), "record_0"));
		attributes.putAttribute("rel-arr", relArr);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isForbidden());
	}

	@Test
	@Transactional
	void referencingMissingTableFails() throws Exception {
		RecordType referencedType = RecordType.valueOf("missing");
		RecordType referringType = RecordType.valueOf("ref_samples-2");
		createSomeRecords(referringType, 1);
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.putAttribute("sample-ref", ref);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
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
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_99");
		attributes.putAttribute("sample-ref", ref);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isForbidden())
				.andExpect(result -> assertTrue(result.getResolvedException() instanceof InvalidRelationException));
	}

	@Test
	@Transactional
	void expandColumnDefForNewData() throws Exception {
		RecordType recordType = RecordType.valueOf("to-alter");
		createSomeRecords(recordType, 1);
		RecordAttributes attributes = RecordAttributes.empty();
		String newTextValue = "convert this column from date to text";
		attributes.putAttribute("attr3", newTextValue);
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_1").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isCreated()).andExpect(jsonPath("$.attributes.attr3", is(newTextValue)));
	}

	@Test
	@Transactional
	void patchMissingRecord() throws Exception {
		RecordType recordType = RecordType.valueOf("to-patch");
		createSomeRecords(recordType, 1);
		RecordAttributes attributes = RecordAttributes.empty();
		attributes.putAttribute("attr-boolean", true);
		String recordId = "record_missing";
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, recordId).content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void putRecordWithMissingTableReference() throws Exception {
		String recordType = "record-type-missing-table-ref";
		String recordId = "record_0";
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), "missing_also");
		attributes.putAttribute("sample-ref", ref);

		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, recordId).content(mapper.writeValueAsString(new RecordRequest(attributes)))
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
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, recordId);
		attributes.putAttribute("ref-attr", ref);
		// Add referencing attribute to referring_Type
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, recordId).content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk());
		// Create a new referring_Type that puts a reference to a non-existent
		// recordType in the pre-existing referencing attribute
		RecordAttributes new_attributes = RecordAttributes.empty();
		String invalid_ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), recordId);
		new_attributes.putAttribute("ref-attr", invalid_ref);

		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "new_record").content(mapper.writeValueAsString(new RecordRequest(new_attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isBadRequest());
	}

	@Test
	@Transactional
	void tryToAssignReferenceToNonRefColumn() throws Exception {
		RecordType recordType = RecordType.valueOf("ref-alter");
		createSomeRecords(recordType, 1);
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(RecordType.valueOf("missing"), "missing_also");
		attributes.putAttribute("attr1", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "record_0").content(mapper.writeValueAsString(new RecordRequest(attributes)))
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
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.putAttribute("sample-ref", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
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
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.putAttribute("sample-ref", ref);
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
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
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.putAttribute("sample-ref", ref);
		// Create relation column
		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				referringType, "record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
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
		RecordAttributes attributes = RecordAttributes.empty();
		String ref = RelationUtils.createRelationString(referencedType, "record_0");
		attributes.putAttribute("attr-ref", ref);

		mockMvc.perform(patch("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId, type,
				"record_0").contentType(MediaType.APPLICATION_JSON)
						.content(mapper.writeValueAsString(new RecordRequest(attributes))))
				.andExpect(status().isOk()).andExpect(content().string(containsString(ref)));

		List<AttributeSchema> expectedAttributes = Arrays.asList(
				new AttributeSchema("array-of-date", "ARRAY_OF_DATE", null),
				new AttributeSchema("array-of-datetime", "ARRAY_OF_DATE_TIME", null),
				new AttributeSchema("array-of-string", "ARRAY_OF_STRING", null),
				new AttributeSchema("attr-boolean", "BOOLEAN", null),
				new AttributeSchema("attr-dt", "DATE_TIME", null), new AttributeSchema("attr-json", "JSON", null),
				new AttributeSchema("attr-ref", "RELATION", referencedType),
				new AttributeSchema("attr1", "STRING", null), new AttributeSchema("attr2", "NUMBER", null),
				new AttributeSchema("attr3", "DATE", null), new AttributeSchema("attr4", "STRING", null),
				new AttributeSchema("attr5", "NUMBER", null), new AttributeSchema("z-array-of-boolean", "ARRAY_OF_BOOLEAN", null),
				new AttributeSchema("z-array-of-number-double", "ARRAY_OF_NUMBER", null),
				new AttributeSchema("z-array-of-number-long", "ARRAY_OF_NUMBER", null), new AttributeSchema("z-array-of-string", "ARRAY_OF_STRING", null));

		RecordTypeSchema expected = new RecordTypeSchema(type, expectedAttributes, 1);

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, type))
				.andExpect(status().isOk()).andReturn();

		RecordTypeSchema actual = mapper.readValue(mvcResult.getResponse().getContentAsString(),
				RecordTypeSchema.class);

		assertEquals(expected, actual);
	}

	@Test
	@Transactional
	void incompatibleArrayWritesShouldChangeToStringArray() throws Exception {
		String recordType = "test-type";
		List<Record> someRecords = createSomeRecords(recordType, 1);
		RecordRequest recordRequest = new RecordRequest(someRecords.get(0).getAttributes().putAttribute("array-of-date", List.of("should switch to array of string")));
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
						recordType, "new_id").content(mapper.writeValueAsString(recordRequest))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().is2xxSuccessful());
		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}/{type}", instanceId, versionId, recordType))
				.andExpect(status().isOk()).andReturn();
		RecordTypeSchema actual = mapper.readValue(mvcResult.getResponse().getContentAsString(),
				RecordTypeSchema.class);
		assertEquals("ARRAY_OF_STRING", actual.attributes().get(0).datatype());

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
		RecordType type1 = RecordType.valueOf("firstType");
		createSomeRecords(type1, 1, instanceId);
		RecordType type2 = RecordType.valueOf("secondType");
		createSomeRecords(type2, 2, instanceId);
		RecordType type3 = RecordType.valueOf("thirdType");
		createSomeRecords(type3, 10, instanceId);

		List<AttributeSchema> expectedAttributes = Arrays.asList(new AttributeSchema("array-of-date", "ARRAY_OF_DATE", null),
				new AttributeSchema("array-of-datetime", "ARRAY_OF_DATE_TIME", null),
				new AttributeSchema("array-of-string", "ARRAY_OF_STRING", null), new AttributeSchema("attr-boolean", "BOOLEAN", null),
				new AttributeSchema("attr-dt", "DATE_TIME", null), new AttributeSchema("attr-json", "JSON", null),
				new AttributeSchema("attr1", "STRING", null), new AttributeSchema("attr2", "NUMBER", null),
				new AttributeSchema("attr3", "DATE", null), new AttributeSchema("attr4", "STRING", null),
				new AttributeSchema("attr5", "NUMBER", null), new AttributeSchema("z-array-of-boolean", "ARRAY_OF_BOOLEAN", null),
				new AttributeSchema("z-array-of-number-double", "ARRAY_OF_NUMBER", null),
				new AttributeSchema("z-array-of-number-long", "ARRAY_OF_NUMBER", null), new AttributeSchema("z-array-of-string", "ARRAY_OF_STRING", null));

		List<RecordTypeSchema> expectedSchemas = Arrays.asList(new RecordTypeSchema(type1, expectedAttributes, 1),
				new RecordTypeSchema(type2, expectedAttributes, 2),
				new RecordTypeSchema(type3, expectedAttributes, 10));

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{v}", instanceId, versionId))
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

	private List<Record> createSomeRecords(String recordType, int numRecords) throws Exception {
		return createSomeRecords(RecordType.valueOf(recordType), numRecords, instanceId);
	}

	private List<Record> createSomeRecords(RecordType recordType, int numRecords) throws Exception {
		return createSomeRecords(recordType, numRecords, instanceId);
	}

	private List<Record> createSomeRecords(RecordType recordType, int numRecords, UUID instId) throws Exception {
		List<Record> result = new ArrayList<>();
		for (int i = 0; i < numRecords; i++) {
			String recordId = "record_" + i;
			RecordAttributes attributes = generateRandomAttributes();
			RecordRequest recordRequest = new RecordRequest(attributes);
			mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instId, versionId,
					recordType, recordId).content(mapper.writeValueAsString(recordRequest))
							.contentType(MediaType.APPLICATION_JSON))
					.andExpect(status().is2xxSuccessful());
			result.add(new Record(recordId, recordType, recordRequest));
		}
		return result;
	}

	@Test
	@Transactional
	void batchWriteInsertShouldSucceed() throws Exception {
		String recordId = "foo";
		String newBatchRecordType = "new-record-type";
		Record record = new Record(recordId, RecordType.valueOf(newBatchRecordType),
				new RecordAttributes(Map.of("attr1", "attr-val")));
		Record record2 = new Record("foo2", RecordType.valueOf(newBatchRecordType),
				new RecordAttributes(Map.of("attr1", "attr-val")));
		BatchOperation op = new BatchOperation(record, OperationType.UPSERT);
		mockMvc.perform(post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, newBatchRecordType)
				.content(mapper.writeValueAsString(List.of(op, new BatchOperation(record2, OperationType.UPSERT))))
				.contentType(MediaType.APPLICATION_JSON)).andExpect(jsonPath("$.recordsModified", is(2)))
				.andExpect(jsonPath("$.message", is("Huzzah"))).andExpect(status().isOk());
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				newBatchRecordType, recordId).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());
		mockMvc.perform(post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, newBatchRecordType)
				.content(mapper.writeValueAsString(List.of(new BatchOperation(record, OperationType.DELETE))))
				.contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				newBatchRecordType, recordId).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void batchInsertShouldFailWithInvalidRelation() throws Exception {
		RecordType recordType = RecordType.valueOf("relationBatchInsert");
		List<BatchOperation> batchOperations = List.of(
				new BatchOperation(
						new Record("record_0", recordType,
								new RecordAttributes(Map.of("attr-relation",
										RelationUtils.createRelationString(RecordType.valueOf("missing"), "A")))),
						OperationType.UPSERT),
				new BatchOperation(
						new Record("record_1", recordType,
								new RecordAttributes(Map.of("attr-relation",
										RelationUtils.createRelationString(RecordType.valueOf("missing"), "A")))),
						OperationType.UPSERT));
		mockMvc.perform(post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, recordType)
				.content(mapper.writeValueAsString(batchOperations)).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void batchInsertShouldFailWithInvalidRelationExistingRecordType() throws Exception {
		RecordType recordType = RecordType.valueOf("relationBatchInsert");
		createSomeRecords(recordType, 2);
		List<BatchOperation> batchOperations = List.of(
				new BatchOperation(
						new Record("record_0", recordType,
								new RecordAttributes(Map.of("attr-relation",
										RelationUtils.createRelationString(RecordType.valueOf("missing"), "A")))),
						OperationType.UPSERT),
				new BatchOperation(
						new Record("record_1", recordType,
								new RecordAttributes(Map.of("attr-relation",
										RelationUtils.createRelationString(RecordType.valueOf("missing"), "A")))),
						OperationType.UPSERT));
		mockMvc.perform(post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, recordType)
				.content(mapper.writeValueAsString(batchOperations)).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isNotFound());
	}

	@Test
	@Transactional
	void mixOfUpsertAndDeleteShouldSucceed() throws Exception {
		RecordType recordType = RecordType.valueOf("forBatch");
		List<Record> records = createSomeRecords(recordType, 2);
		Record upsertRcd = records.get(1);
		upsertRcd.getAttributes().putAttribute("new-col", "new value!!");
		List<BatchOperation> ops = List.of(new BatchOperation(records.get(0), OperationType.DELETE),
				new BatchOperation(upsertRcd, OperationType.UPSERT));
		mockMvc.perform(post("/{instanceid}/batch/{v}/{type}", instanceId, versionId, recordType)
				.content(mapper.writeValueAsString(ops)).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk());
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, records.get(0).getId()).contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isNotFound());
		mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, upsertRcd.getId()).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk())
				.andExpect(jsonPath("$.attributes.new-col", is("new value!!")));
	}

	@Test
	@Transactional
	void dateAttributeShouldBeHumanReadable() throws Exception {
		// N.B. This test does not assert that the date attribute is saved as a date in
		// Postgres;
		// other tests verify that.
		RecordType recordType = RecordType.valueOf("test-type");
		RecordAttributes attributes = RecordAttributes.empty();
		String dateString = "1911-01-21";
		attributes.putAttribute("dateAttr", dateString);
		// create record in db
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "recordId").content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		// retrieve as single record
		MvcResult mvcSingleResult = mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}",
				instanceId, versionId, recordType, "recordId")).andExpect(status().isOk()).andReturn();
		// assert single-record response is human-readable
		RecordResponse actualSingle = mapper.readValue(mvcSingleResult.getResponse().getContentAsString(),
				RecordResponse.class);
		assertEquals(dateString, actualSingle.recordAttributes().getAttributeValue("dateAttr"));

		// retrieve as a page of records
		MvcResult mvcMultiResult = mockMvc
				.perform(post("/{instanceId}/search/{version}/{recordType}", instanceId, versionId, recordType))
				.andExpect(status().isOk()).andReturn();

		RecordQueryResponse actualMulti = mapper.readValue(mvcMultiResult.getResponse().getContentAsString(),
				RecordQueryResponse.class);
		assertEquals(dateString, actualMulti.records().get(0).recordAttributes().getAttributeValue("dateAttr"));
	}

	@Test
	@Transactional
	void datetimeAttributeShouldBeHumanReadable() throws Exception {
		// N.B. This test does not assert that the datetime attribute is saved as a
		// timestamp in Postgres;
		// other tests verify that.
		RecordType recordType = RecordType.valueOf("test-type");
		RecordAttributes attributes = RecordAttributes.empty();
		String datetimeString = "1911-01-21T13:45:43";
		attributes.putAttribute("datetimeAttr", datetimeString);
		// create record in db
		mockMvc.perform(put("/{instanceId}/records/{version}/{recordType}/{recordId}", instanceId, versionId,
				recordType, "recordId").content(mapper.writeValueAsString(new RecordRequest(attributes)))
						.contentType(MediaType.APPLICATION_JSON))
				.andExpect(status().isCreated());
		// retrieve as single record
		MvcResult mvcSingleResult = mockMvc.perform(get("/{instanceId}/records/{version}/{recordType}/{recordId}",
				instanceId, versionId, recordType, "recordId")).andExpect(status().isOk()).andReturn();
		// assert single-record response is human-readable
		RecordResponse actualSingle = mapper.readValue(mvcSingleResult.getResponse().getContentAsString(),
				RecordResponse.class);
		assertEquals(datetimeString, actualSingle.recordAttributes().getAttributeValue("datetimeAttr"));

		// retrieve as a page of records
		MvcResult mvcMultiResult = mockMvc
				.perform(post("/{instanceId}/search/{version}/{recordType}", instanceId, versionId, recordType))
				.andExpect(status().isOk()).andReturn();

		RecordQueryResponse actualMulti = mapper.readValue(mvcMultiResult.getResponse().getContentAsString(),
				RecordQueryResponse.class);
		assertEquals(datetimeString, actualMulti.records().get(0).recordAttributes().getAttributeValue("datetimeAttr"));
	}

}
