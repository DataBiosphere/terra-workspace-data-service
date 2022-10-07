package org.databiosphere.workspacedataservice;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.InBoundDataSource;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
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
	void inferTypesTsvSource() {
		Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeAttrs(), InBoundDataSource.TSV);
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
		assertThat(inferer.inferType("[\"12345\"]", InBoundDataSource.JSON)).isEqualTo(DataTypeMapping.JSON);
	}

	private static RecordAttributes getSomeAttrs() {
		return new RecordAttributes(Map.of("int_val", new Random().nextInt(), "string_val",
				RandomStringUtils.random(10), "json_val", "[\"a\", \"b\"]", "date_val", "2001-11-03", "date_time_val",
				"2001-11-03T10:00:00", "number_or_string", "47"));
	}
}
