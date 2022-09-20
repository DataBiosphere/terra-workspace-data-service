package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;

public class TestUtils {

	private TestUtils() {

	}

	public static RecordAttributes generateRandomAttributes() {
		RecordAttributes attributes = RecordAttributes.empty();
		attributes.putAttribute("attr1", RandomStringUtils.randomAlphabetic(6));
		attributes.putAttribute("attr2", RandomUtils.nextFloat());
		attributes.putAttribute("attr3", "2022-11-01");
		attributes.putAttribute("attr4", RandomStringUtils.randomNumeric(5));
		attributes.putAttribute("attr5", RandomUtils.nextLong());
		attributes.putAttribute("attr-dt", "2022-03-01T12:00:03");
		attributes.putAttribute("attr-json", "{\"foo\":\"bar\"}");
		attributes.putAttribute("attr-boolean", true);
		return attributes;
	}
}
