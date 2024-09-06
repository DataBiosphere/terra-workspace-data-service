package org.databiosphere.workspacedataservice.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.service.MDCServletRequestListener;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"spring.main.allow-bean-definition-overriding=true"})
class FullStackMDCRequestResponseTest extends ControlPlaneTestBase {

  // hmmmmm https://github.com/gradle/gradle/issues/5975

  @Autowired TestRestTemplate restTemplate;

  private final String instanceId = UUID.randomUUID().toString();
  private static final String versionId = "v0.2";

  @Test
  void responseShouldContainUniqueId() {
    HttpHeaders headers = new HttpHeaders();
    HttpEntity<String> requestEntity = new HttpEntity<>(headers);
    ResponseEntity<String> resp =
        restTemplate.exchange(
            "/{instanceId}/types/{version}",
            HttpMethod.GET,
            requestEntity,
            String.class,
            instanceId,
            versionId);

    List<String> actualResponseHeaders =
        resp.getHeaders().get(MDCServletRequestListener.RESPONSE_HEADER);
    assertNotNull(actualResponseHeaders);
    assertEquals(1, actualResponseHeaders.size());
    assertThat(actualResponseHeaders.get(0), CoreMatchers.not(emptyOrNullString()));
  }

  // "strings" input should match MDCFilter.INCOMING_HEADERS
  @ParameterizedTest(name = "Trace ID in header {0} should be honored")
  @ValueSource(strings = {"x-b3-traceid", "x-request-id", "trace-id"})
  void traceIdInHeaderShouldBeHonored(String requestHeaderName) {
    String requestHeaderValue = RandomStringUtils.randomAlphanumeric(32);
    HttpHeaders headers = new HttpHeaders();
    headers.add(requestHeaderName, requestHeaderValue);

    HttpEntity<String> requestEntity = new HttpEntity<>(headers);
    ResponseEntity<String> resp =
        restTemplate.exchange(
            "/{instanceId}/types/{version}",
            HttpMethod.GET,
            requestEntity,
            String.class,
            instanceId,
            versionId);

    List<String> actualResponseHeaders =
        resp.getHeaders().get(MDCServletRequestListener.RESPONSE_HEADER);
    assertNotNull(actualResponseHeaders);
    assertEquals(1, actualResponseHeaders.size());

    assertEquals(requestHeaderValue, actualResponseHeaders.get(0));
  }

  @ParameterizedTest(name = "A blank Trace ID in header {0} should NOT be honored")
  @ValueSource(strings = {"x-b3-traceid", "x-request-id", "trace-id"})
  void emptyTraceIdInHeaderShouldNotBeHonored(String requestHeaderName) {
    HttpHeaders headers = new HttpHeaders();
    headers.add(requestHeaderName, " "); // just a space value

    HttpEntity<String> requestEntity = new HttpEntity<>(headers);
    ResponseEntity<String> resp =
        restTemplate.exchange(
            "/{instanceId}/types/{version}",
            HttpMethod.GET,
            requestEntity,
            String.class,
            instanceId,
            versionId);

    List<String> actualResponseHeaders =
        resp.getHeaders().get(MDCServletRequestListener.RESPONSE_HEADER);
    assertNotNull(actualResponseHeaders);
    assertEquals(1, actualResponseHeaders.size());
    assertThat(actualResponseHeaders.get(0), CoreMatchers.not(emptyOrNullString()));
  }

  @Test
  void multipleTraceIdsInHeadersShouldFollowOurPriority() {
    String requestHeaderValue = RandomStringUtils.randomAlphanumeric(32);

    HttpHeaders headers = new HttpHeaders();
    headers.add("x-request-id", "ignoreme");
    headers.add("trace-id", "ignoremetoo");
    headers.add("x-b3-traceid", requestHeaderValue); // highest priority

    HttpEntity<String> requestEntity = new HttpEntity<>(headers);
    ResponseEntity<String> resp =
        restTemplate.exchange(
            "/{instanceId}/types/{version}",
            HttpMethod.GET,
            requestEntity,
            String.class,
            instanceId,
            versionId);

    List<String> actualResponseHeaders =
        resp.getHeaders().get(MDCServletRequestListener.RESPONSE_HEADER);
    assertNotNull(actualResponseHeaders);
    assertEquals(1, actualResponseHeaders.size());
    assertEquals(requestHeaderValue, actualResponseHeaders.get(0));
  }

  @Test
  void longTraceIdsInHeadersShouldBeShortened() {
    String requestHeaderValue = RandomStringUtils.randomAlphanumeric(256);

    HttpHeaders headers = new HttpHeaders();
    headers.add("x-request-id", requestHeaderValue);

    HttpEntity<String> requestEntity = new HttpEntity<>(headers);
    ResponseEntity<String> resp =
        restTemplate.exchange(
            "/{instanceId}/types/{version}",
            HttpMethod.GET,
            requestEntity,
            String.class,
            instanceId,
            versionId);

    List<String> actualResponseHeaders =
        resp.getHeaders().get(MDCServletRequestListener.RESPONSE_HEADER);
    assertNotNull(actualResponseHeaders);
    assertEquals(1, actualResponseHeaders.size());
    assertEquals(64, actualResponseHeaders.get(0).length());
    assertTrue(requestHeaderValue.startsWith(actualResponseHeaders.get(0)));
  }
}
