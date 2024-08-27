package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrentDataTypeChangesTest extends TestBase {
  @Autowired private TestRestTemplate restTemplate;
  private HttpHeaders headers;
  private UUID instanceId;

  private static final String recordId = "concurrent-changes";
  private static final RecordType recordType = RecordType.valueOf("concurrency");
  private static final String versionId = "v0.2";

  @Autowired private TwdsProperties twdsProperties;

  @BeforeEach
  void setUp() throws JsonProcessingException {
    String name = "test-name";
    String description = "test-description";

    CollectionRequestServerModel collectionRequestServerModel =
        new CollectionRequestServerModel(name, description);

    ObjectMapper objectMapper = new ObjectMapper();

    headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> response =
        restTemplate.exchange(
            "/collections/v1/{workspaceId}",
            HttpMethod.POST,
            new HttpEntity<>(
                objectMapper.writeValueAsString(collectionRequestServerModel), headers),
            String.class,
            twdsProperties.workspaceId().id());
    assertEquals(HttpStatus.CREATED, response.getStatusCode());

    instanceId =
        TestUtils.getCollectionId(objectMapper, Objects.requireNonNull(response.getBody()));
  }

  @AfterEach
  void tearDown() {
    ResponseEntity<String> response =
        restTemplate.exchange(
            "/collections/v1/{workspaceId}/{instanceid}",
            HttpMethod.DELETE,
            new HttpEntity<>("", headers),
            String.class,
            twdsProperties.workspaceId().id(),
            instanceId);
    assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
  }

  @Test
  void concurrentColumnCreation() {
    // concurrency factor for reads + writes
    int numIterations = 20;

    // create the initial record, with no attributes
    RecordRequest recordRequest = new RecordRequest(RecordAttributes.empty());
    ResponseEntity<String> response =
        restTemplate.exchange(
            "/{instanceid}/records/{v}/{type}/{id}",
            HttpMethod.PUT,
            new HttpEntity<>(recordRequest, headers),
            String.class,
            instanceId,
            versionId,
            recordType.getName(),
            recordId);
    assertEquals(HttpStatus.CREATED, response.getStatusCode());

    /* for each iteration in numIterations,
    create two API requests. The first API request is a GET to read the record,
    the second API request is a PATCH to add a new attribute to the record.
    Under concurrent load, this is likely to generate an
    "ERROR: cached plan must not change result type" exception.
    Wrap these requests in CompletableFutures, so we can execute them in parallel.
    */
    Stream<CompletableFuture<ResponseEntity<String>>> futures =
        IntStream.rangeClosed(1, numIterations)
            .boxed()
            .flatMap(
                i -> {
                  CompletableFuture<ResponseEntity<String>> writeRequest =
                      CompletableFuture.supplyAsync(() -> writeOrThrow(i));
                  CompletableFuture<ResponseEntity<String>> readRequest =
                      CompletableFuture.supplyAsync(() -> readOrThrow(i));
                  return Stream.of(readRequest, writeRequest);
                });

    // execute all the requests in parallel (as much parallelism as we can).
    // we don't care about the responses, we only care if this throws errors.
    CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    assertDoesNotThrow(() -> combinedFuture.get());

    // finally, verify that the record has all the columns and values
    ResponseEntity<RecordResponse> finalResponse =
        restTemplate.exchange(
            "/{instanceid}/records/{v}/{type}/{id}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            RecordResponse.class,
            instanceId,
            versionId,
            recordType.getName(),
            recordId);

    // create our expected attributes
    Map<String, Object> attrs = new HashMap<>();
    IntStream.rangeClosed(1, numIterations)
        .boxed()
        .forEach(i -> attrs.put("attr-" + i, "value-" + i));
    // the final record will include the extra attribute "sys_name=concurrent-changes" because of
    // the primary key
    attrs.put("sys_name", recordId);
    RecordAttributes expected = new RecordAttributes(attrs);

    RecordAttributes actual = finalResponse.getBody().recordAttributes();

    assertEquals(
        expected.attributeSet().size(),
        actual.attributeSet().size(),
        "final record should have " + expected.attributeSet().size() + " attributes");

    assertEquals(expected, actual);
  }

  private ResponseEntity<String> writeOrThrow(int iteration) {
    // add a new attribute for each PUT request
    RecordAttributes attrs =
        new RecordAttributes(Map.of("attr-" + iteration, "value-" + iteration));
    RecordRequest req = new RecordRequest(attrs);

    ResponseEntity<String> response =
        restTemplate.exchange(
            "/{instanceid}/records/{v}/{type}/{id}",
            HttpMethod.PATCH,
            new HttpEntity<>(req, headers),
            String.class,
            instanceId,
            versionId,
            recordType.getName(),
            recordId);
    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new RuntimeException(
          "Exception on write iteration "
              + iteration
              + " with response body: "
              + response.getBody());
    }
    return response;
  }

  private ResponseEntity<String> readOrThrow(int iteration) {
    ResponseEntity<String> response =
        restTemplate.exchange(
            "/{instanceid}/records/{v}/{type}/{id}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            String.class,
            instanceId,
            versionId,
            recordType.getName(),
            recordId);
    if (!response.getStatusCode().is2xxSuccessful()) {
      throw new RuntimeException(
          "Exception on read iteration "
              + iteration
              + " with response body: "
              + response.getBody());
    }
    return response;
  }
}
