package org.databiosphere.workspacedataservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedata.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.InBoundDataSource;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class DataTypeInfererTest {

	DataTypeInferer inferer = new DataTypeInferer();

	@Test
	void inferTypesJsonSource() {
		Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeAttrs(), InBoundDataSource.JSON);
		Map<String, DataTypeMapping> expected = new HashMap<>();
		expected.put("string_val", DataTypeMapping.STRING);
		expected.put("int_val", DataTypeMapping.LONG);
		expected.put("json_val", DataTypeMapping.JSON);
		expected.put("date_val", DataTypeMapping.DATE);
		expected.put("date_time_val", DataTypeMapping.DATE_TIME);
		expected.put("number_or_string", DataTypeMapping.STRING);

		assertEquals(expected, result);
	}

	@Test
	void selectBestTypes() {
		assertThat(inferer.selectBestType(DataTypeMapping.LONG, DataTypeMapping.DOUBLE))
				.as("longs can be expressed as doubles but not the other way around").isEqualTo(DataTypeMapping.DOUBLE);
		assertThat(inferer.selectBestType(DataTypeMapping.NULL, DataTypeMapping.DOUBLE))
				.as("null values should not affect typing for non null values").isEqualTo(DataTypeMapping.DOUBLE);
		assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.STRING))
				.as("if types are identical, return the type").isEqualTo(DataTypeMapping.STRING);
		assertThat(inferer.selectBestType(DataTypeMapping.STRING, DataTypeMapping.BOOLEAN))
				.as("should generalize to string/text type").isEqualTo(DataTypeMapping.STRING);
	}

	@Test
	void inferTypesTsvSource() {
		Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeTsvAttrs(), InBoundDataSource.TSV);
		Map<String, DataTypeMapping> expected = new HashMap<>();
		expected.put("string_val", DataTypeMapping.STRING);
		expected.put("int_val", DataTypeMapping.LONG);
		expected.put("json_val", DataTypeMapping.JSON);
		expected.put("date_val", DataTypeMapping.DATE);
		expected.put("date_time_val", DataTypeMapping.DATE_TIME);
		expected.put("number_or_string", DataTypeMapping.LONG);

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
				.isEqualTo(Map.of("boolean", DataTypeMapping.BOOLEAN, "long", DataTypeMapping.LONG));
	}

	@Test
	void nullValuesMixedWithNonNullsShouldStillYieldProperTypeJSON() {
		RecordAttributes firstAttrs = RecordAttributes.empty().putAttribute("boolean", null).putAttribute("long", null);
		Record first = new Record("first", RecordType.valueOf("test-inference"), firstAttrs);
		RecordAttributes secondAttrs = RecordAttributes.empty().putAttribute("boolean", "true").putAttribute("long",
				-999999);
		Record second = new Record("second", RecordType.valueOf("test-inference"), secondAttrs);
		Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(List.of(first, second),
				InBoundDataSource.JSON);
		assertThat(inferredSchema).as("Should still get BOOLEAN and LONG for types despite null values in one record")
				.isEqualTo(Map.of("boolean", DataTypeMapping.BOOLEAN, "long", DataTypeMapping.LONG));
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
		assertThat(inferer.inferType("[\"12345\"]", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.ARRAY_OF_TEXT);
	}

	private static RecordAttributes getSomeAttrs() {
		return new RecordAttributes(
				Map.of("int_val", 4747, "string_val", "Abracadabra Open Sesame", "json_val", "{\"list\": [\"a\", \"b\"]}",
						"date_val", "2001-11-03", "date_time_val", "2001-11-03T10:00:00", "number_or_string", "47"));
	}

	private static RecordAttributes getSomeTsvAttrs() {
		return new RecordAttributes(
				Map.of("int_val", "4747", "string_val", "Abracadabra Open Sesame", "json_val", "{\"list\": [\"a\", \"b\"]}",
						"date_val", "2001-11-03", "date_time_val", "2001-11-03T10:00:00", "number_or_string", "47"));
	}
}
