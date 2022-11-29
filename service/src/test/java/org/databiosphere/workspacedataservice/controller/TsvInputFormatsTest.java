package org.databiosphere.workspacedataservice.controller;

import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class TsvInputFormatsTest {

	@Autowired
	private MockMvc mockMvc;
	@Autowired
	RecordDao recordDao;

	private static UUID instanceId;

	private static final String versionId = "v0.2";

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

	private static BigDecimal[] toBigDecimals(int[] nums) {
		return Arrays.stream(nums).mapToObj(BigDecimal::valueOf).toList().toArray(new BigDecimal[nums.length]);
	}

	private static void evaluateOutputs(Object expected, Object actual) {
		if (expected == null) {
			assertNull(actual, "Attribute should be null");
		} else {
			assertInstanceOf(expected.getClass(), actual, "Attribute should be of expected type");

			if (expected.getClass().isArray()) {
				assertArrayEquals((Object[])expected, (Object[])actual);
			} else {
				assertEquals(expected, actual);
			}
		}
	}

	private static Stream<Arguments> provideInputFormats() {
		/* Arguments are sets:
			- first value is the text that would be contained in a TSV cell
			- second value is the expected Java type that WDS would create, after saving the TSV and re-retrieving the record.
			- third value is the text that would be contained in a subsequent TSV cell upload (e.g. the user uploads a file once, then adds more data later)
				- such value would be the same datatype; however, in some cases, WDS can ingest the value even if represented slightly differently
		 */
		return Stream.of(
				Arguments.of("", null, ""),
				Arguments.of(" ", " ", " "),
				Arguments.of("true", Boolean.TRUE, "TRUE"),
				Arguments.of("TRUE", Boolean.TRUE, "true"),
				Arguments.of("tRuE", Boolean.TRUE, "true"),
				Arguments.of("True", Boolean.TRUE, "true"),
				Arguments.of("false", Boolean.FALSE, "FALSE"),
				Arguments.of("FALSE", Boolean.FALSE, "false"),
				Arguments.of("fAlSe", Boolean.FALSE, "false"),
				Arguments.of("False", Boolean.FALSE, "false"),
				Arguments.of(
						"[\"true\", \"TRUE\", \"tRuE\"]",
						new Boolean[]{Boolean.TRUE, Boolean.TRUE, Boolean.TRUE},
						"[\"TRUE\", \"true\", \"True\"]"
				),
				Arguments.of(
						"[\"true\", \"false\", \"true\"]",
						new Boolean[]{Boolean.TRUE, Boolean.FALSE, Boolean.TRUE},
						"[\"TRUE\", \"fALSE\", \"True\"]"
				),
				Arguments.of("5", BigDecimal.valueOf(5), "5"),
				Arguments.of("5.67", BigDecimal.valueOf(5.67d), "5.67"),
				Arguments.of("005", BigDecimal.valueOf(5),"005"),
				Arguments.of("[1,5]", toBigDecimals(new int[]{1,5}), "[1,5]"),
				Arguments.of("[1,5.67]", new BigDecimal[]{BigDecimal.valueOf(1), BigDecimal.valueOf(5.67)}, "[1,5.67]")
				// TODO: array of numbers with leading zeros
				// TODO: smart-quotes?
				// TODO: string-escaping for special characters
				// TODO: relations, arrays of relations
				// TODO: dates, arrays of dates
				// TODO: datetimes, arrays of datetimes
		);
	}


	@Transactional
	@ParameterizedTest(name = "TSV parsing for value {0} should result in {1}, allowing {2} to be subsequently processed")
	@MethodSource("provideInputFormats")
	void testTSVInputFormatTest(String initialInput, Object expected, String subsequentInput) throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				("sys_name\tinput\n" + 1 + "\t" + initialInput + "\n").getBytes());

		String recordType = RandomStringUtils.randomAlphabetic(16);
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
				.file(file)).andExpect(status().isOk());

		// Get newly added Record in table
		Optional<Record> recOption = recordDao.getSingleRecord(instanceId, RecordType.valueOf(recordType), "1");
		assertTrue(recOption.isPresent(), "Record should exist after TSV input");

		Object actual = recOption.get().getAttributeValue("input");

		// Run assertion
		evaluateOutputs(expected, actual);

		MockMultipartFile subsequentFile = new MockMultipartFile("records", "subsequent.tsv", MediaType.TEXT_PLAIN_VALUE,
				("sys_name\tinput\n" + 2 + "\t" + subsequentInput + "\n").getBytes());

		recordType = RandomStringUtils.randomAlphabetic(16);
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
				.file(subsequentFile)).andExpect(status().isOk());

		recOption = recordDao.getSingleRecord(instanceId, RecordType.valueOf(recordType), "2");
		assertTrue(recOption.isPresent(), "Record should exist after TSV input");

		actual = recOption.get().getAttributeValue("input");

		evaluateOutputs(expected, actual);
	}

//	/* TODO: add a second unit test, or a second class, that asserts behavior of various inputs to a table that already
//		exists and whose schema is already defined.
//	 */
//	@Transactional
//	@ParameterizedTest(name = "TSV parsing for value {0} should result in {1}")
//	@MethodSource("provideInputFormats")
//	void testTSVExistingTableTest(String input, Object expected) throws Exception {
//
//		// Create initial upload
//		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
//				("sys_name\tinput\n" + 1 + "\t" + input + "\n").getBytes());
//
//		String recordType = RandomStringUtils.randomAlphabetic(16);
//		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
//				.file(file)).andExpect(status().isOk());
//
//		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
//				.file(file)).andExpect(status().isOk());
//	}

}
