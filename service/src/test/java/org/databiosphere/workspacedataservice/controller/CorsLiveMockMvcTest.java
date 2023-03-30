package org.databiosphere.workspacedataservice.controller;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * This class tests CORS behavior for live deployments. WDS in live deployments sits behind
 * Azure Relay, and Relay handles CORS headers. Therefore, WDS should *not* reply with CORS
 * headers, as it will cause a conflict.
 *
 * See also CorsLocalMockMvcTest for testing CORS behavior in the "local" Spring profile
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
@AutoConfigureMockMvc
class CorsLiveMockMvcTest {

	@Autowired
	MockMvc mockMvc;

	private static final String versionId = "v0.2";

	@ParameterizedTest(name = "CORS response headers for non-local profile to {0} should be correct")
	@ValueSource(strings = {"/instances/{version}/{instanceId}", "/{instanceid}/types/{v}",
			"/{instanceid}/tsv/{v}/{type}", "/{instanceid}/search/{v}/{type}", "/{instanceid}/records/{v}/{type}/{id}"})
	void testCorsResponseHeaders(String urlTemplate) throws Exception {
		UUID uuid = UUID.randomUUID();

		// for the non-"local" profile, OPTIONS requests should return 403 and not include
		// Access-Control-Allow-Origin or Access-Control-Allow-Methods headers.

		MvcResult mvcResult = mockMvc.perform(options(urlTemplate, versionId, uuid, "sometype", "someid")
				// Access-Control-Request-Method and Origin required to trigger CORS response
				.header("Access-Control-Request-Method", "POST").header("Origin", "http://www.example.com"))
				.andExpect(status().isForbidden()).andReturn();

		String actualOrigin = mvcResult.getResponse().getHeader("Access-Control-Allow-Origin");
		assertNull(actualOrigin, "Access-Control-Allow-Origin should not be present in CORS response for " + urlTemplate);

		String actualMethods = mvcResult.getResponse().getHeader("Access-Control-Allow-Methods");
		assertNull(actualMethods, "Access-Control-Allow-Methods should not be present in CORS response for " + urlTemplate);
	}

}
