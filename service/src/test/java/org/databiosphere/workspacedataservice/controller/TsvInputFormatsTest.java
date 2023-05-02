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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"}) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
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
	void afterEach() {
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
				Arguments.of("\"false\"", Boolean.FALSE, "\"fAlSe\""),
				Arguments.of(
						"[true]",
						new Boolean[]{Boolean.TRUE},
						"[true]"
				),
				Arguments.of(
						"[true, TRUE, tRuE]",
						new Boolean[]{Boolean.TRUE, Boolean.TRUE, Boolean.TRUE},
						"[TRUE, true, True]"
				),
				Arguments.of(
						"[true, false, true]",
						new Boolean[]{Boolean.TRUE, Boolean.FALSE, Boolean.TRUE},
						"[TRUE, fALSE, True]"
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
		// Construct TSV with initial input values
		MockMultipartFile file = new MockMultipartFile("records", "first_upload.tsv", MediaType.TEXT_PLAIN_VALUE,
				("sys_name\tinput\n" + 1 + "\t" + initialInput + "\n").getBytes());

		// Upload TSV first time
		String recordType = RandomStringUtils.randomAlphabetic(16);
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
				.file(file)).andExpect(status().isOk());

		// Get newly added Record in table
		Optional<Record> recOption = recordDao.getSingleRecord(instanceId, RecordType.valueOf(recordType), "1");
		assertTrue(recOption.isPresent(), "Record should exist after TSV input");

		// Run assertion on output
		Object actual = recOption.get().getAttributeValue("input");
		evaluateOutputs(expected, actual);

		// Construct TSV with different input values
		MockMultipartFile subsequentFile = new MockMultipartFile("records", "subsequent_upload.tsv", MediaType.TEXT_PLAIN_VALUE,
				("sys_name\tinput\n" + 2 + "\t" + subsequentInput + "\n").getBytes());

		// Upload TSV second time, where it will need to abide by schema generated during first TSV upload
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
				.file(subsequentFile)).andExpect(status().isOk());

		// Get newly added Record in table
		recOption = recordDao.getSingleRecord(instanceId, RecordType.valueOf(recordType), "2");
		assertTrue(recOption.isPresent(), "Record should exist after TSV input");

		// Run assertion on output -- ensure that classes remain consistent
		actual = recOption.get().getAttributeValue("input");
		if (expected == null) {
			assertNull(actual, "Attribute should be null");
		} else {
			assertEquals(actual.getClass(), expected.getClass());
		}
	}

}
