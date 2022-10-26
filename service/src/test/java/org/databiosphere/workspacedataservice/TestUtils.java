package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class TestUtils {

	private TestUtils() {
	}

	public static RecordAttributes generateRandomAttributes() {
		return RecordAttributes.empty().putAttribute("attr1", RandomStringUtils.randomAlphabetic(6))
				.putAttribute("attr2", RandomUtils.nextFloat()).putAttribute("attr3", "2022-11-01")
				.putAttribute("attr4", RandomStringUtils.randomNumeric(5)).putAttribute("attr5", RandomUtils.nextLong())
				.putAttribute("attr-dt", "2022-03-01T12:00:03").putAttribute("attr-json", "{\"foo\":\"bar\"}")
				.putAttribute("attr-boolean", "TruE")
				.putAttribute("z-array-of-number-double", List.of(99.9, 88, -77.1, 47, 47))
				.putAttribute("z-array-of-boolean", List.of("True", "False", false))
				.putAttribute("z-array-of-number-long", "[1,2,3,4,5,80000001]")
				.putAttribute("z-array-of-string", "[\"Ross\", \"Chandler\", \"Joey\"]")
				.putAttribute("array-of-date", List.of(LocalDate.of(1776, 7, 4), LocalDate.of(1999, 12, 31)))
				.putAttribute("array-of-datetime",
						List.of(LocalDateTime.of(2021, 1, 6, 13, 30), LocalDateTime.of(1980, 10, 31, 23, 59)))
				.putAttribute("array-of-string", List.of("Ross", "Chandler", "Joey"));
	}

	public static RecordAttributes generateNonRandomAttributes(int seed) {
		return RecordAttributes.empty().putAttribute("attr1", getStringBySeed(seed))
				.putAttribute("attr2", getFloatBySeed(seed)).putAttribute("attr3", getIntBySeed(seed))
				.putAttribute("attr-dt", getDateBySeed(seed));
	}

	private static String getStringBySeed(int seed) {
		return List.of("abc", "def", "ghi", "jkl", "mno", "pqr", "stu", "vwx").get(seed % 8);
	}
	private static BigInteger getIntBySeed(int seed) {
		return new BigInteger(List.of(3, 1, 4, 15, 9, 2, 6, 5).get(seed % 8).toString());
	}
	private static BigDecimal getFloatBySeed(int seed) {
		// In order ascending order: 6.626070e-34f, 1.602e-19f, 6.674e-11f, 1.4142f,
		// 1.61803f, 2.7182f, 3.14159f, 2.99792448e8f
		return new BigDecimal(
				List.of(3.14159f, 2.7182f, 1.4142f, 1.61803f, 6.674e-11f, 2.99792448e8f, 6.626070e-34f, 1.602e-19f)
						.get(seed % 8).toString());
	}
	private static String getDateBySeed(int seed) {
		return List
				.of("2022-03-01T12:00:01", "2019-07-21T12:00:01", "1987-01-10T12:00:01", "2002-03-23T12:00:01",
						"1999-12-31T12:00:01", "2022-04-25T12:00:01", "2016-06-06T12:00:01", "1991-05-15T12:00:01")
				.get(seed % 8);
	}
}
