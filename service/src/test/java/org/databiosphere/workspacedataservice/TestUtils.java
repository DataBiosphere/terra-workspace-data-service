package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;

import java.util.HashMap;
import java.util.Map;

public class TestUtils {

	private TestUtils() {

	}

	public static RecordAttributes generateRandomAttributes() {
		RecordAttributes attributes = RecordAttributes.empty();
		attributes.put("attr1", RandomStringUtils.randomAlphabetic(6));
		attributes.put("attr2", RandomUtils.nextFloat());
		attributes.put("attr3", "2022-11-01");
		attributes.put("attr4", RandomStringUtils.randomNumeric(5));
		attributes.put("attr5", RandomUtils.nextLong());
		attributes.put("attr-dt", "2022-03-01T12:00:03");
		attributes.put("attr-json", "{\"foo\":\"bar\"}");
		attributes.put("attr-boolean", true);
		return attributes;
	}
}
