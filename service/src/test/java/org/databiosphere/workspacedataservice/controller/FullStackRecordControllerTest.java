package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestUtils.generateNonRandomAttributes;
import static org.databiosphere.workspacedataservice.TestUtils.generateRandomAttributes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedata.model.ErrorResponse;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.RawlsProtectedDataSupport;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.shared.model.BatchOperation;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.databiosphere.workspacedataservice.shared.model.SortDirection;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * This test spins up a web server and the full Spring Boot web stack. It was necessary to add it in
 * order to test error handling since MockMvc doesn't match full Spring Boot error handling: <a
 * href="https://github.com/spring-projects/spring-framework/issues/17290">https://github.com/spring-projects/spring-framework/issues/17290</a>
 * As a result, this test suite is currently focused on validating expected error handling
 */
@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"spring.main.allow-bean-definition-overriding=true"})
@Import(SmallBatchWriteTestConfig.class)
class FullStackRecordControllerTest extends ControlPlaneTestBase {
  @Autowired private CollectionService collectionService;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TestRestTemplate restTemplate;
  @Autowired private WorkspaceRepository workspaceRepository;

  @MockitoBean RawlsProtectedDataSupport rawlsProtectedDataSupport;

  private HttpHeaders headers;
  private UUID instanceId;
  private static final String versionId = "v0.2";
  @Autowired private ObjectMapper mapper;

  @BeforeEach
  void setUp() throws JsonProcessingException {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // create the workspace record
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));

    assertThat(workspaceRepository.existsById(workspaceId)).isTrue();

    // RawlsProtectedDataSupport makes calls to Rawls; mock it to avoid the Rawls call
    when(rawlsProtectedDataSupport.workspaceSupportsProtectedDataPolicy(workspaceId))
        .thenReturn(false);

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
            workspaceId);
    assertEquals(HttpStatus.CREATED, response.getStatusCode());

    instanceId =
        TestUtils.getCollectionId(objectMapper, Objects.requireNonNull(response.getBody()));
  }

  @AfterEach
  void tearDown() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  @Test
  void testBadRecordTypeNames() throws JsonProcessingException {
    HttpEntity<String> requestEntity =
        new HttpEntity<>(
            mapper.writeValueAsString(new RecordRequest(RecordAttributes.empty())), headers);
    List<String> badNames = List.of("); drop table users;", "$$foo.bar", "...", "&Q$(*^@$(*");
    for (String badName : badNames) {
      ResponseEntity<ErrorResponse> response =
          restTemplate.exchange(
              "/{instanceId}/records/{version}/{recordType}/{recordId}",
              HttpMethod.PUT,
              requestEntity,
              ErrorResponse.class,
              instanceId,
              versionId,
              badName,
              "sample_1");
      ErrorResponse err = assertInstanceOf(ErrorResponse.class, response.getBody());

      assertThat(err.getMessage())
          .containsPattern("Record Type .* or contain characters besides letters");
      assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
  }

  @Test
  void testQuerySuccess() throws Exception {
    RecordType recordType = RecordType.valueOf("for_query");
    List<String> names =
        List.of(
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R", "S", "T", "U", "V", "W", "X", "Y", "Z");
    Iterator<String> namesIterator = names.iterator();
    createSomeRecords(recordType, 26, namesIterator::next);
    int limit = 5;
    int offset = 0;
    RecordQueryResponse body = executeExpectedSuccess(recordType);
    assertThat(body.records())
        .as("When no search request POST body is sent, we should use defaults")
        .hasSize(10);
    body = executeExpectedSuccess(recordType, new SearchRequest(limit, offset, SortDirection.ASC));
    assertThat(body.records()).hasSize(limit);
    assertThat(body.records().get(0).recordId())
        .as("A should be the first record id in ascending order")
        .isEqualTo("A");
    assertThat(body.records().get(4).recordId()).isEqualTo("E");
    body = executeExpectedSuccess(recordType, new SearchRequest(limit, offset, SortDirection.DESC));
    assertThat(body.records()).hasSize(limit);
    assertThat(body.records().get(0).recordId())
        .as("Z should be first record id in descending order")
        .isEqualTo("Z");
    assertThat(body.records().get(4).recordId()).isEqualTo("V");
    offset = 10;
    body = executeExpectedSuccess(recordType, new SearchRequest(limit, offset, SortDirection.ASC));
    assertThat(body.records().get(0).recordId())
        .as("K should be first record id in ascending order with offset of 10")
        .isEqualTo("K");
  }

  @Test
  void testSortedQuerySuccess() throws Exception {
    RecordType recordType = RecordType.valueOf("sorted_query");
    createSpecificRecords(recordType, 8);
    int limit = 5;
    int offset = 0;
    SearchRequest sortByAlpha = new SearchRequest(limit, offset, SortDirection.ASC, "attr1");
    RecordQueryResponse body = executeExpectedSuccess(recordType, sortByAlpha);
    assertThat(body.records()).hasSize(limit);
    assertThat(body.records().get(0).recordAttributes().getAttributeValue("attr1"))
        .as("Record with attr1 'abc' should be first record in ascending order")
        .isEqualTo("abc");
    assertThat(body.records().get(4).recordAttributes().getAttributeValue("attr1"))
        .isEqualTo("mno");
    SearchRequest sortByFloat = new SearchRequest(limit, offset, SortDirection.DESC, "attr2");
    body = executeExpectedSuccess(recordType, sortByFloat);
    assertThat(body.records()).hasSize(limit);
    assertThat(body.records().get(0).recordAttributes().getAttributeValue("attr2"))
        .as("Record with attr2 2.99792448e8f should be first record in descending order")
        .isEqualTo(new BigInteger("299792448"));
    assertThat(body.records().get(4).recordAttributes().getAttributeValue("attr2"))
        .isEqualTo(new BigDecimal("1.4142"));
    SearchRequest sortByInt = new SearchRequest(limit, offset, SortDirection.ASC, "attr3");
    body = executeExpectedSuccess(recordType, sortByInt);
    assertThat(body.records().get(0).recordAttributes().getAttributeValue("attr3"))
        .as("Record with attr3 1 should be first record in ascending order")
        .isEqualTo(new BigInteger("1"));
    assertThat(body.records().get(4).recordAttributes().getAttributeValue("attr3"))
        .isEqualTo(new BigInteger("5"));
    SearchRequest sortByDate = new SearchRequest(limit, offset, SortDirection.DESC, "attr-dt");
    body = executeExpectedSuccess(recordType, sortByDate);
    assertThat(body.records()).hasSize(limit);

    assertThat(body.records().get(0).recordAttributes().getAttributeValue("attr-dt"))
        .as("Record with attr-dt 2022-04-25 should be first record in descending order")
        .isEqualTo("2022-04-25T12:00:01");
    assertThat(body.records().get(4).recordAttributes().getAttributeValue("attr-dt"))
        .isEqualTo("2002-03-23T12:00:01");
  }

  @Test
  void testQueryFailures() throws Exception {
    RecordType recordType = RecordType.valueOf("for_query");
    int limit = 5;
    int offset = 0;
    ResponseEntity<ErrorResponse> response =
        executeExpectedError(recordType, new SearchRequest(limit, offset, SortDirection.ASC));
    assertThat(response.getStatusCode())
        .as("record type doesn't exist")
        .isEqualTo(HttpStatus.NOT_FOUND);
    createSomeRecords(recordType, 1);
    response =
        executeExpectedError(
            recordType, new SearchRequest(limit, offset, SortDirection.ASC, "no-attr"));
    assertThat(response.getStatusCode())
        .as("attribute doesn't exist")
        .isEqualTo(HttpStatus.NOT_FOUND);
    limit = 1001;
    response =
        executeExpectedError(recordType, new SearchRequest(limit, offset, SortDirection.ASC));
    assertThat(response.getStatusCode())
        .as("unsupported limit size")
        .isEqualTo(HttpStatus.BAD_REQUEST);
    limit = 0;
    response =
        executeExpectedError(recordType, new SearchRequest(limit, offset, SortDirection.ASC));
    assertThat(response.getStatusCode())
        .as("unsupported limit size")
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }

  private <T> ResponseEntity<T> executeQuery(
      RecordType recordType, Class<T> responseType, SearchRequest... request)
      throws JsonProcessingException {
    HttpEntity<String> requestEntity =
        new HttpEntity<>(
            request != null && request.length > 0 ? mapper.writeValueAsString(request[0]) : "",
            headers);
    return restTemplate.exchange(
        "/{instanceid}/search/{v}/{type}",
        HttpMethod.POST,
        requestEntity,
        responseType,
        instanceId,
        versionId,
        recordType);
  }

  private RecordQueryResponse executeExpectedSuccess(
      RecordType recordType, SearchRequest... request) throws JsonProcessingException {
    ResponseEntity<RecordQueryResponse> response =
        executeQuery(recordType, RecordQueryResponse.class, request);
    return assertInstanceOf(RecordQueryResponse.class, response.getBody());
  }

  private ResponseEntity<ErrorResponse> executeExpectedError(
      RecordType recordType, SearchRequest... request) throws JsonProcessingException {
    ResponseEntity<ErrorResponse> response = executeQuery(recordType, ErrorResponse.class, request);
    assertInstanceOf(ErrorResponse.class, response.getBody());
    return response;
  }

  @Test
  void testBadAttributeNames() throws JsonProcessingException {
    List<String> badNames =
        List.of("create table buttheads(id int)", "samples\n11", "##magic beans!");
    for (String badName : badNames) {
      RecordAttributes attributes = RecordAttributes.empty();
      attributes.putAttribute(badName, "foo");
      HttpEntity<String> requestEntity =
          new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(attributes)), headers);
      ResponseEntity<ErrorResponse> response =
          restTemplate.exchange(
              "/{instanceId}/records/{version}/{recordType}/{recordId}",
              HttpMethod.PUT,
              requestEntity,
              ErrorResponse.class,
              instanceId,
              versionId,
              "sample",
              "sample_1");
      ErrorResponse err = response.getBody();
      assertNotNull(err);
      assertThat(err.getMessage())
          .containsPattern("Attribute .* or contain characters besides letters");
      assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
  }

  @Test
  void missingReferencedRecordTypeShouldFail() throws JsonProcessingException {
    RecordAttributes attrs = RecordAttributes.empty();
    attrs.putAttribute(
        "attr_ref",
        RelationUtils.createRelationString(RecordType.valueOf("non_existent"), "recordId"));
    attrs.putAttribute(
        "attr_ref_2",
        RelationUtils.createRelationString(RecordType.valueOf("non_existent_2"), "recordId"));
    HttpEntity<String> requestEntity =
        new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(attrs)), headers);
    ResponseEntity<ErrorResponse> response =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.PUT,
            requestEntity,
            ErrorResponse.class,
            instanceId,
            versionId,
            "samples-1",
            "sample_1");
    ErrorResponse err = response.getBody();
    assertNotNull(err);
    assertThat(err.getMessage())
        .isEqualTo(
            "Record type for relation does not exist or you do not have permission to see it");
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void referencingMissingRecordShouldFail() throws Exception {
    RecordAttributes attrs = RecordAttributes.empty();
    RecordType referencedRecordType = RecordType.valueOf("referenced-type");
    createSomeRecords(referencedRecordType, 1);
    attrs.putAttribute(
        "attr_ref", RelationUtils.createRelationString(referencedRecordType, "missing-id"));
    HttpEntity<String> requestEntity =
        new HttpEntity<>(mapper.writeValueAsString(new RecordRequest(attrs)), headers);
    ResponseEntity<ErrorResponse> response =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.PUT,
            requestEntity,
            ErrorResponse.class,
            instanceId,
            versionId,
            "samples-2",
            "sample_1");
    ErrorResponse responseContent = response.getBody();
    assertNotNull(responseContent);
    assertThat(responseContent.getMessage())
        .isEqualTo("It looks like you're trying to reference a record that does not exist.");
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  void retrievingMissingEntityShouldFail() throws Exception {
    createSomeRecords(RecordType.valueOf("samples"), 1);
    HttpEntity<String> requestEntity = new HttpEntity<>(headers);
    ResponseEntity<ErrorResponse> response =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.GET,
            requestEntity,
            ErrorResponse.class,
            instanceId,
            versionId,
            "samples",
            "sample_1");
    ErrorResponse responseContent = response.getBody();
    assertNotNull(responseContent);
    assertThat(responseContent.getMessage())
        .isEqualTo("Record does not exist or you do not have permission to see it");
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void invalidApiVersionShouldFail() {
    ResponseEntity<LinkedHashMap> response =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            LinkedHashMap.class,
            instanceId,
            "garbage",
            "type",
            "id");
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    var body = assertInstanceOf(LinkedHashMap.class, response.getBody());
    assertThat(body).containsEntry("message", "Invalid API version specified");
  }

  private List<Record> createSomeRecords(RecordType recordType, int numRecords) throws Exception {
    return createSomeRecords(recordType, numRecords, null);
  }

  private List<Record> createSomeRecords(
      RecordType recordType, int numRecords, Supplier<String> recordIdSupplier) throws Exception {
    List<Record> result = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      String recordId = recordIdSupplier == null ? "record_" + i : recordIdSupplier.get();
      RecordAttributes attributes = generateRandomAttributes();
      RecordRequest recordRequest = new RecordRequest(attributes);
      ResponseEntity<String> response =
          restTemplate.exchange(
              "/{instanceId}/records/{version}/{recordType}/{recordId}",
              HttpMethod.PUT,
              new HttpEntity<>(mapper.writeValueAsString(recordRequest), headers),
              String.class,
              instanceId,
              versionId,
              recordType,
              recordId);
      assertThat(response.getStatusCode()).isIn(HttpStatus.CREATED, HttpStatus.OK);
      result.add(new Record(recordId, recordType, recordRequest));
    }
    return result;
  }

  private List<Record> createSpecificRecords(RecordType recordType, int numRecords)
      throws Exception {
    List<Record> result = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      String recordId = "record_" + i;
      RecordAttributes attributes = generateNonRandomAttributes(i);
      RecordRequest recordRequest = new RecordRequest(attributes);
      ResponseEntity<String> response =
          restTemplate.exchange(
              "/{instanceId}/records/{version}/{recordType}/{recordId}",
              HttpMethod.PUT,
              new HttpEntity<>(mapper.writeValueAsString(recordRequest), headers),
              String.class,
              instanceId,
              versionId,
              recordType,
              recordId);
      assertThat(response.getStatusCode()).isIn(HttpStatus.CREATED, HttpStatus.OK);
      result.add(new Record(recordId, recordType, recordRequest));
    }
    return result;
  }

  @Test
  void dataTypeMismatchShouldNotFailBatchWrite() throws Exception {
    RecordType recordType = RecordType.valueOf("bw-test");
    List<Record> someRecords = createSomeRecords(recordType, 2);
    List<BatchOperation> operations =
        someRecords.stream()
            .map(
                r ->
                    new BatchOperation(
                        new Record(r.getId(), r.getRecordType(), r.getAttributes()),
                        OperationType.UPSERT))
            .toList();
    operations
        .get(1)
        .getRecord()
        .getAttributes()
        .putAttribute("attr2", "not a float, this should not fail");
    ResponseEntity<ErrorResponse> response =
        restTemplate.exchange(
            "/{instanceid}/batch/{v}/{type}",
            HttpMethod.POST,
            new HttpEntity<>(mapper.writeValueAsString(operations), headers),
            ErrorResponse.class,
            instanceId,
            versionId,
            recordType);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void batchDeletingReferencedRecordsShouldFail() throws Exception {
    RecordType referencedRT = RecordType.valueOf("batch-delete-test-referenced");
    List<Record> someRecords = createSomeRecords(referencedRT, 2);
    RecordType referencerRT = RecordType.valueOf("referencer");
    List<BatchOperation> ops =
        List.of(
            new BatchOperation(
                new Record(
                    "referencer-1",
                    referencerRT,
                    new RecordAttributes(
                        Map.of(
                            "attr-ref",
                            RelationUtils.createRelationString(referencedRT, "record_0")))),
                OperationType.UPSERT));
    restTemplate.exchange(
        "/{instanceid}/batch/{v}/{type}",
        HttpMethod.POST,
        new HttpEntity<>(mapper.writeValueAsString(ops), headers),
        String.class,
        instanceId,
        versionId,
        referencerRT);
    List<BatchOperation> deleteOps =
        List.of(
            new BatchOperation(someRecords.get(1), OperationType.DELETE),
            new BatchOperation(someRecords.get(0), OperationType.DELETE));

    ResponseEntity<ErrorResponse> error =
        restTemplate.exchange(
            "/{instanceid}/batch/{v}/{type}",
            HttpMethod.POST,
            new HttpEntity<>(mapper.writeValueAsString(deleteOps), headers),
            ErrorResponse.class,
            instanceId,
            versionId,
            referencedRT);
    ErrorResponse err = error.getBody();
    assertNotNull(err);
    assertThat(err.getMessage()).contains("because another record has a relation to it");
    ResponseEntity<RecordResponse> stillPresentNonReferencedRecord =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            RecordResponse.class,
            instanceId,
            versionId,
            referencedRT,
            "record_1");
    var body = assertInstanceOf(RecordResponse.class, stillPresentNonReferencedRecord.getBody());
    assertThat(body.recordId()).isEqualTo("record_1");
  }

  @Test
  void batchDeleteShouldFailWhenRecordIsNotFound() throws Exception {
    RecordType recordType = RecordType.valueOf("forBatchDelete");
    createSomeRecords(recordType, 3);
    RecordAttributes emptyAtts = new RecordAttributes(new HashMap<>());
    List<BatchOperation> batchOperations =
        List.of(
            new BatchOperation(new Record("record_0", recordType, emptyAtts), OperationType.DELETE),
            new BatchOperation(new Record("missing", recordType, emptyAtts), OperationType.DELETE));
    ResponseEntity<ErrorResponse> error =
        restTemplate.exchange(
            "/{instanceid}/batch/{v}/{type}",
            HttpMethod.POST,
            new HttpEntity<>(mapper.writeValueAsString(batchOperations), headers),
            ErrorResponse.class,
            instanceId,
            versionId,
            recordType);
    assertThat(error.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    // record_0 should still be present since the above deletion is transactional
    // and should fail upon 'missing'
    ResponseEntity<RecordResponse> response =
        restTemplate.exchange(
            "/{instanceId}/records/{version}/{recordType}/{recordId}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            RecordResponse.class,
            instanceId,
            versionId,
            recordType,
            "record_0");
    var body = assertInstanceOf(RecordResponse.class, response.getBody());
    assertThat(body.recordId()).isEqualTo("record_0");
  }
}
