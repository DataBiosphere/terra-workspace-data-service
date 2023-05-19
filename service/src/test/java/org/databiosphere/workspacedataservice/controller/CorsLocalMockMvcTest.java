package org.databiosphere.workspacedataservice.controller;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * This class tests CORS behavior for local development. WDS in local development does not sit behind
 * Azure Relay, and therefore does not inherit Relay's CORS responseheaders. By enabling the "local"
 * Spring profile, WDS sends its own CORS response headers.
 *
 * See also CorsLiveMockMvcTest for testing CORS behavior in live deployments.
 */
@ActiveProfiles(profiles = {"local"})
@DirtiesContext
@SpringBootTest
@AutoConfigureMockMvc
class CorsLocalMockMvcTest {

	@Autowired
	MockMvc mockMvc;

	private static final String versionId = "v0.2";

	@ParameterizedTest(name = "CORS response headers for the local profile {0} should be correct")
	@ValueSource(strings = {"/instances/{version}/{instanceId}", "/{instanceid}/types/{v}",
			"/{instanceid}/tsv/{v}/{type}", "/{instanceid}/search/{v}/{type}", "/{instanceid}/records/{v}/{type}/{id}"})
	void testCorsResponseHeaders(String urlTemplate) throws Exception {
		UUID uuid = UUID.randomUUID();

		// for the "local" profile, OPTIONS requests should return 200 and include
		// Access-Control-Allow-Origin and Access-Control-Allow-Methods headers.

		MvcResult mvcResult = mockMvc.perform(options(urlTemplate, versionId, uuid, "sometype", "someid")
				// Access-Control-Request-Method and Origin required to trigger CORS response
				.header("Access-Control-Request-Method", "POST").header("Origin", "http://www.example.com"))
				.andExpect(status().isOk()).andReturn();

		String actualOrigin = mvcResult.getResponse().getHeader("Access-Control-Allow-Origin");
		assertNotNull(actualOrigin, "Access-Control-Allow-Origin not present in CORS response for " + urlTemplate);
		assertEquals("*", actualOrigin, "bad Access-Control-Allow-Origin in CORS response for " + urlTemplate);

		String actualMethods = mvcResult.getResponse().getHeader("Access-Control-Allow-Methods");
		assertNotNull(actualMethods, "Access-Control-Allow-Methods not present in CORS response for " + urlTemplate);
		assertEquals("DELETE,GET,HEAD,PATCH,POST,PUT", actualMethods,
				"bad Access-Control-Allow-Methods in CORS response for " + urlTemplate);

	}

}
