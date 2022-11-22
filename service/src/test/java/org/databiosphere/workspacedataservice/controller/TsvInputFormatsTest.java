package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class TsvInputFormatsTest {

	@Autowired
	private ObjectMapper mapper;
	@Autowired
	private MockMvc mockMvc;
	@Autowired
	RecordDao recordDao;

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

	private static BigDecimal[] toBigDecimals(int[] nums) {
		return Arrays.stream(nums).mapToObj(BigDecimal::valueOf).toList().toArray(new BigDecimal[nums.length]);
	}

	private static BigDecimal[] toBigDecimals(double[] nums) {
		return Arrays.stream(nums).mapToObj(BigDecimal::valueOf).toList().toArray(new BigDecimal[nums.length]);
	}

	private static Stream<Arguments> provideInputFormats() {
		return Stream.of(
				Arguments.of("", null),
				Arguments.of(" ", " "),
				Arguments.of("true", Boolean.TRUE),
				Arguments.of("TRUE", Boolean.TRUE),
				Arguments.of("tRuE", Boolean.TRUE),
				Arguments.of("[true, TRUE, tRuE]", new Boolean[]{Boolean.TRUE, Boolean.TRUE, Boolean.TRUE}), // TODO: fails
				Arguments.of("5", BigDecimal.valueOf(5)),
				Arguments.of("5.67", BigDecimal.valueOf(5.67d)),
				Arguments.of("005", BigDecimal.valueOf(5)),
				Arguments.of("[1,5]", toBigDecimals(new int[]{1,5})),
				Arguments.of("[1,5.67]", new BigDecimal[]{BigDecimal.valueOf(1), BigDecimal.valueOf(5.67)}),
				Arguments.of("[1,005]", toBigDecimals(new int[]{1,5})) // TODO: fails
				// TODO: smart-quotes?
				// TODO: string-escaping for special characters
				// TODO: relations, arrays of relations
				// TODO: dates, arrays of dates
				// TODO: datetimes, arrays of datetimes
		);
	}


	@Transactional
	@ParameterizedTest(name = "TSV parsing for value {0} should result in {1}")
	@MethodSource("provideInputFormats")
	void tsvInputFormatTest(String input, Object expected) throws Exception {
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				("sys_name\tinput\n" + 1 + "\t" + input + "\n").getBytes());

		String recordType = RandomStringUtils.randomAlphabetic(16);
		mockMvc.perform(multipart("/{instanceId}/tsv/{version}/{recordType}", instanceId, versionId, recordType)
				.file(file)).andExpect(status().isOk());

		Optional<Record> recOption = recordDao.getSingleRecord(instanceId, RecordType.valueOf(recordType), "1");
		assertTrue(recOption.isPresent(), "Record should exist after TSV input");

		Object actual = recOption.get().getAttributeValue("input");

		// special handling for nulls
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


}
