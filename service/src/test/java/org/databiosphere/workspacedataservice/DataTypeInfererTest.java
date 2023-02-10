package org.databiosphere.workspacedataservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.InBoundDataSource;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DataTypeInfererTest {

	@Autowired
	DataTypeInferer inferer;

	@Test
	void inferTypesJsonSource() {
		Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeAttrs(), InBoundDataSource.JSON);
		Map<String, DataTypeMapping> expected = new HashMap<>();
		expected.put("array_of_string", DataTypeMapping.ARRAY_OF_STRING);
		expected.put("string_val", DataTypeMapping.STRING);
		expected.put("int_val", DataTypeMapping.NUMBER);
		expected.put("json_val", DataTypeMapping.JSON);
		expected.put("date_val", DataTypeMapping.DATE);
		expected.put("date_time_val", DataTypeMapping.DATE_TIME);
		expected.put("file_val", DataTypeMapping.FILE);
		expected.put("number_or_string", DataTypeMapping.STRING);
		expected.put("relation", DataTypeMapping.RELATION);
		expected.put("rel_arr", DataTypeMapping.ARRAY_OF_RELATION);
		expected.put("array_of_file", DataTypeMapping.ARRAY_OF_FILE);
		assertEquals(expected, result);
	}

	@Test
	void selectBestTypes() {
		assertThat(inferer.selectBestType(DataTypeMapping.NULL, DataTypeMapping.NUMBER))
				.as("null values should not affect typing for non null values").isEqualTo(DataTypeMapping.NUMBER);
		assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.STRING))
				.as("if types are identical, return the type").isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.BOOLEAN))
				.as("should generalize to string/text type").isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.selectBestType(DataTypeMapping.DATE, DataTypeMapping.DATE_TIME))
				.as("should convert date to datetime").isEqualTo(DataTypeMapping.DATE_TIME);
		assertThat(inferer.selectBestType(DataTypeMapping.ARRAY_OF_DATE, DataTypeMapping.ARRAY_OF_DATE_TIME))
				.as("should convert array of date to array of datetime").isEqualTo(DataTypeMapping.ARRAY_OF_DATE_TIME);
		assertThat(inferer.selectBestType(DataTypeMapping.ARRAY_OF_STRING, DataTypeMapping.ARRAY_OF_RELATION))
				.as("should convert array of relation to array of string").isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
		assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.RELATION))
				.as("should convert relation to string").isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.selectBestType(DataTypeMapping.FILE, DataTypeMapping.STRING))
				.as("should convert file to string").isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.selectBestType(DataTypeMapping.ARRAY_OF_FILE, DataTypeMapping.ARRAY_OF_STRING))
				.as("should convert array of file to array of string").isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
	}

	@Test
	void inferTypesTsvSource() {
		Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeTsvAttrs(), InBoundDataSource.TSV);
		Map<String, DataTypeMapping> expected = new HashMap<>();
		expected.put("string_val", DataTypeMapping.STRING);
		expected.put("int_val", DataTypeMapping.NUMBER);
		expected.put("json_val", DataTypeMapping.JSON);
		expected.put("date_val", DataTypeMapping.DATE);
		expected.put("date_time_val", DataTypeMapping.DATE_TIME);
		expected.put("number_or_string", DataTypeMapping.NUMBER);
		expected.put("relation", DataTypeMapping.RELATION);
		expected.put("rel_arr", DataTypeMapping.ARRAY_OF_RELATION);
		expected.put("file_val", DataTypeMapping.FILE);
		expected.put("array_of_file", DataTypeMapping.ARRAY_OF_FILE);
		assertEquals(expected, result);
	}

	@Test
	void isValidJson() {
		assertThat(inferer.isValidJson(RandomStringUtils.randomNumeric(10))).isFalse();
		assertThat(inferer.isValidJson("Hello")).isFalse();
		assertThat(inferer.isValidJson(Boolean.TRUE.toString())).isFalse();
		assertThat(inferer.isValidJson("True")).isFalse();
		assertThat(inferer.isValidJson("{\"foo\":\"bar\"}")).isTrue();
	}

	@Test
	void nullValuesMixedWithNonNullsShouldStillYieldProperTypeTSV() {
		RecordAttributes firstAttrs = RecordAttributes.empty().putAttribute("boolean", "").putAttribute("long", null);
		Record first = new Record("first", RecordType.valueOf("test-inference"), firstAttrs);
		RecordAttributes secondAttrs = RecordAttributes.empty().putAttribute("boolean", "true").putAttribute("long",
				"-999999");
		Record second = new Record("second", RecordType.valueOf("test-inference"), secondAttrs);
		Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(List.of(first, second), InBoundDataSource.TSV);
		assertThat(inferredSchema).as("Should still get BOOLEAN and LONG for types despite null values in one record")
				.isEqualTo(Map.of("boolean", DataTypeMapping.BOOLEAN, "long", DataTypeMapping.NUMBER));
	}

	@Test
	void nullValuesMixedWithNonNullsShouldStillYieldProperTypeJSON() {
		RecordAttributes firstAttrs = RecordAttributes.empty().putAttribute("boolean", null).putAttribute("long", null);
		Record first = new Record("first", RecordType.valueOf("test-inference"), firstAttrs);
		RecordAttributes secondAttrs = RecordAttributes.empty().putAttribute("boolean", "true").putAttribute("long",
				new BigInteger("-999999"));
		Record second = new Record("second", RecordType.valueOf("test-inference"), secondAttrs);
		Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(List.of(first, second),
				InBoundDataSource.JSON);
		assertThat(inferredSchema).as("Should still get BOOLEAN and LONG for types despite null values in one record")
				.isEqualTo(Map.of("boolean", DataTypeMapping.BOOLEAN, "long", DataTypeMapping.NUMBER));
	}

	@Test
	void inferSomeTypes() {
		assertThat(inferer.inferType("True", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.BOOLEAN);
		assertThat(inferer.inferType("Hello", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.inferType("2020-01-01", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.DATE);
		assertThat(inferer.inferType("2020-01-01T00:10:00", InBoundDataSource.JSON))
				.isEqualTo(DataTypeMapping.DATE_TIME);
		assertThat(inferer.inferType("2020-01-01T00:10:00", InBoundDataSource.JSON))
				.isEqualTo(DataTypeMapping.DATE_TIME);
		assertThat(inferer.inferType("12345", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.inferType("12345A", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.inferType(List.of("Hello!"), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
		assertThat(inferer.inferType(List.of(new BigInteger("12345")), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_NUMBER);
		assertThat(inferer.inferType(List.of(true, false, true), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_BOOLEAN);
		assertThat(inferer.inferType(List.of(new BigDecimal("11.1"), new BigDecimal("12"), new BigDecimal("14")), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_NUMBER);
		assertThat(inferer.inferType(List.of(RelationUtils.createRelationString(RecordType.valueOf("testType"),"recordId"), RelationUtils.createRelationString(RecordType.valueOf("testType"),"recordId2"), RelationUtils.createRelationString(RecordType.valueOf("testType"),"recordId3")), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_RELATION);
		assertThat(inferer.inferType(List.of(RelationUtils.createRelationString(RecordType.valueOf("testType"),"recordId"), "not a relation string", RelationUtils.createRelationString(RecordType.valueOf("testType"),"recordId3")), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
		assertThat(inferer.inferType(RelationUtils.createRelationString(RecordType.valueOf("testType"),"recordId3"), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.RELATION);
		assertThat(inferer.inferType("https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/my%20file.pdf", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.FILE);
	}

	@Test
	void ambiguousConversions() {
		assertThat(inferer.inferType(List.of(true, "false", "True"), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_BOOLEAN);
		assertThat(inferer.inferType(List.of("11", "99"), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
		assertThat(inferer.inferType(List.of("11", new BigDecimal("99"), "foo"), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
		assertThat(inferer.inferType("", InBoundDataSource.TSV)).isEqualTo(DataTypeMapping.NULL);
		assertThat(inferer.inferType("", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.inferType(List.of(new BigInteger("11"), new BigInteger("99"), new BigDecimal("-3.14")), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_NUMBER);
		assertThat(inferer.inferType(List.of(new BigInteger("11"), new BigInteger("99"), new BigDecimal("-3.14"), "09"), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_STRING);
		assertThat(inferer.inferType(Collections.emptyList(), InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.EMPTY_ARRAY);
		assertThat(inferer.inferType("[11, 99, -3.14, 09]", InBoundDataSource.TSV)).isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.inferType("[a]", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.inferType("[11, 99, -3.14, 09]", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.STRING);
	}

	private static RecordAttributes getSomeAttrs() {
		return new RecordAttributes(
				Map.ofEntries(
						Map.entry("int_val", new BigDecimal("4747")),
						Map.entry("string_val", "Abracadabra Open Sesame"),
						Map.entry("json_val", "{\"list\": [\"a\", \"b\"]}"),
						Map.entry("date_val", "2001-11-03"),
						Map.entry("date_time_val", "2001-11-03T10:00:00"),
						Map.entry("number_or_string", "47"),
						Map.entry("array_of_string", List.of("red", "yellow")),
						Map.entry("relation", RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId")),
						Map.entry("rel_arr", List.of(RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId"),
								RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId2"),
								RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId3"))),
						Map.entry("file_val", "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/file.cram?param=foo"),
						Map.entry("array_of_file", List.of("https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/notebook.ipynb","drs://jade.datarepo-dev.broadinstitute.org/v1_9545e956-aa6a-4b84-a037-d0ed164c1890"))
				));
	}

	private static RecordAttributes getSomeTsvAttrs() {
		return new RecordAttributes(
				Map.of("int_val", "4747", "string_val", "Abracadabra Open Sesame", "json_val", "{\"list\": [\"a\", \"b\"]}",
						"date_val", "2001-11-03", "date_time_val", "2001-11-03T10:00:00",
						"file_val", "https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/file.bam",
						"array_of_file",
						"[\"https://lz1a2b345c67def8a91234bc.blob.core.windows.net/sc-7ad51c5d-eb4c-4685-bffe-62b861f7753f/file.fasta\",\"drs://jade.datarepo-dev.broadinstitute.org/v1%209545e956%20d0ed164c1890\"]",
						"number_or_string", "47",
						"relation", RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId"),
						"rel_arr", "[\""+RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId")+"\", \""
				+RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "testRecordId2")+"\"]"));
	}
}
