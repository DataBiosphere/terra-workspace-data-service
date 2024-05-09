package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.text.StringSubstitutor;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext
@ActiveProfiles(profiles = "mock-sam")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class NoCacheFilterTest extends TestBase {
  private static final String versionId = "v0.2";

  @Autowired private ObjectMapper mapper;

  @Autowired private TestRestTemplate restTemplate;

  @ParameterizedTest(name = "Responses from {0} should not be cached")
  @ValueSource(
      strings = {
        "/instances/${version}",
        "/${instanceId}/types/${version}",
        "/${instanceId}/tsv/${version}/${recordType}",
        "/${instanceId}/records/${version}/${recordType}/${recordId}"
      })
  void apiResponsesAreNotCached(String urlTemplate) throws Exception {
    // Arrange
    UUID instanceId = UUID.randomUUID();
    String recordType = "record";
    String recordId = "record_1";

    createInstanceAndUploadRecord(instanceId, recordType, recordId);

    URI requestUri =
        URI.create(
            StringSubstitutor.replace(
                urlTemplate,
                Map.of(
                    "version", versionId,
                    "instanceId", instanceId,
                    "recordType", recordType,
                    "recordId", recordId)));

    // Act
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> response =
        restTemplate.exchange(requestUri, HttpMethod.GET, new HttpEntity<>(headers), String.class);

    // Assert
    List<String> cacheControlHeaders = response.getHeaders().get("Cache-Control");
    List<String> pragmaHeaders = response.getHeaders().get("Pragma");

    assertThat(cacheControlHeaders).isEqualTo(List.of("no-store"));
    assertThat(pragmaHeaders).isEqualTo(List.of("no-cache"));
  }

  @ParameterizedTest(name = "Responses from {0} should be cached")
  @ValueSource(
      strings = {
        "/webjars/swagger-ui-dist/swagger-ui-bundle.js",
      })
  void staticResourcesMayBeCached(String url) {
    // Arrange
    URI requestUri = URI.create(url);

    // Act
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> response =
        restTemplate.exchange(requestUri, HttpMethod.GET, new HttpEntity<>(headers), String.class);

    // Assert
    List<String> cacheControlHeaders = response.getHeaders().get("Cache-Control");
    List<String> pragmaHeaders = response.getHeaders().get("Pragma");

    assertThat(cacheControlHeaders).isNull();
    assertThat(pragmaHeaders).isNull();
  }

  private void createInstanceAndUploadRecord(UUID instanceId, String recordType, String recordId)
      throws Exception {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    // Create instance
    ResponseEntity<String> createInstanceResponse =
        restTemplate.exchange(
            "/instances/{version}/{instanceId}",
            HttpMethod.POST,
            new HttpEntity<>(headers),
            String.class,
            versionId,
            instanceId);
    assertEquals(HttpStatus.CREATED, createInstanceResponse.getStatusCode());

    // Create a record
    RecordAttributes attributes = generateRandomAttributes();
    RecordRequest recordRequest = new RecordRequest(attributes);
    ResponseEntity<String> uploadRecordResponse =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.PUT,
            new HttpEntity<>(mapper.writeValueAsString(recordRequest), headers),
            String.class,
            instanceId,
            versionId,
            recordType,
            recordId);
    assertThat(uploadRecordResponse.getStatusCode()).isIn(HttpStatus.CREATED, HttpStatus.OK);
  }
}
