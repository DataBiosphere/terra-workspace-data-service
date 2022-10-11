package org.databiosphere.workspacedataservice.controller;

import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.MDCFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class MDCRequestResponseMockMvcTest {

	// hmmmmm https://github.com/gradle/gradle/issues/5975

	@Autowired
	private MockMvc mockMvc;

	private String instanceId = UUID.randomUUID().toString();
	private static String versionId = "v0.2";

	@Test
	void responseShouldContainUniqueId() throws Exception {
		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{version}", instanceId, versionId))
				.andExpect(status().isNotFound()).andReturn();

		String actualResponseHeader = mvcResult.getResponse().getHeader(MDCFilter.RESPONSE_HEADER);
		assertNotNull(actualResponseHeader);
		assertDoesNotThrow( () -> UUID.fromString(actualResponseHeader) );
	}

	// "strings" input should match MDCFilter.INCOMING_HEADERS
	@ParameterizedTest(name = "Trace ID in header {0} should be honored")
	@ValueSource(strings = {"x-b3-traceid", "x-request-id", "trace-id"})
	void traceIdInHeaderShouldBeHonored(String requestHeaderName) throws Exception {
		String requestHeaderValue = RandomStringUtils.randomAlphanumeric(32);

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{version}", instanceId, versionId)
						.header(requestHeaderName, requestHeaderValue))
				.andExpect(status().isNotFound()).andReturn();

		String actualResponseHeader = mvcResult.getResponse().getHeader(MDCFilter.RESPONSE_HEADER);
		assertNotNull(actualResponseHeader);
		assertEquals(requestHeaderValue, actualResponseHeader);
	}

	@ParameterizedTest(name = "A blank Trace ID in header {0} should NOT be honored")
	@ValueSource(strings = {"x-b3-traceid", "x-request-id", "trace-id"})
	void emptyTraceIdInHeaderShouldNotBeHonored(String requestHeaderName) throws Exception {
		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{version}", instanceId, versionId)
						.header(requestHeaderName, " ")) // just a space character
				.andExpect(status().isNotFound()).andReturn();

		String actualResponseHeader = mvcResult.getResponse().getHeader(MDCFilter.RESPONSE_HEADER);
		assertNotNull(actualResponseHeader);
		assertDoesNotThrow( () -> UUID.fromString(actualResponseHeader) );
	}

	@Test
	void multipleTraceIdsInHeadersShouldFollowOurPriority() throws Exception {
		String requestHeaderValue = RandomStringUtils.randomAlphanumeric(32);

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{version}", instanceId, versionId)
						.header("x-request-id", "ignoreme")
						.header("trace-id", "ignoremetoo")
						.header("x-b3-traceid", requestHeaderValue) // highest priority
				)
				.andExpect(status().isNotFound()).andReturn();

		String actualResponseHeader = mvcResult.getResponse().getHeader(MDCFilter.RESPONSE_HEADER);
		assertNotNull(actualResponseHeader);
		assertEquals(requestHeaderValue, actualResponseHeader);
	}

	@Test
	void longTraceIdsInHeadersShouldBeShortened() throws Exception {
		String requestHeaderValue = RandomStringUtils.randomAlphanumeric(256);

		MvcResult mvcResult = mockMvc.perform(get("/{instanceId}/types/{version}", instanceId, versionId)
						.header("x-request-id", requestHeaderValue)
				)
				.andExpect(status().isNotFound()).andReturn();

		String actualResponseHeader = mvcResult.getResponse().getHeader(MDCFilter.RESPONSE_HEADER);
		assertNotNull(actualResponseHeader);
		assertEquals(64, actualResponseHeader.length());
		assertTrue(requestHeaderValue.startsWith(actualResponseHeader));
	}


}
