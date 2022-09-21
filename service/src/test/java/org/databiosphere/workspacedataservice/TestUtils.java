package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;

public class TestUtils {

	private TestUtils() {

	}

	public static RecordAttributes generateRandomAttributes() {
		return RecordAttributes.empty()
				.putAttribute("attr1", RandomStringUtils.randomAlphabetic(6))
				.putAttribute("attr2", RandomUtils.nextFloat())
				.putAttribute("attr3", "2022-11-01")
				.putAttribute("attr4", RandomStringUtils.randomNumeric(5))
				.putAttribute("attr5", RandomUtils.nextLong())
				.putAttribute("attr-dt", "2022-03-01T12:00:03")
				.putAttribute("attr-json", "{\"foo\":\"bar\"}")
				.putAttribute("attr-boolean", true);
	}
}
