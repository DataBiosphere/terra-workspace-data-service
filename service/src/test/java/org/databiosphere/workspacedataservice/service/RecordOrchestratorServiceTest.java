package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.DeleteAttributeRequestException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.server.ResponseStatusException;

@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@SpringBootTest
class RecordOrchestratorServiceTest {

  @Autowired private InstanceDao instanceDao;
  @Autowired private RecordOrchestratorService recordOrchestratorService;

  private static final UUID INSTANCE = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
  private static final RecordType TEST_TYPE = RecordType.valueOf("test");

  private static final String RECORD_ID = "aNewRecord";
  private static final String TEST_KEY = "test_key";
  private static final String TEST_VAL = "val";

  @BeforeEach
  void setUp() {
    if (!instanceDao.instanceSchemaExists(INSTANCE)) {
      instanceDao.createSchema(INSTANCE);
    }
  }

  @AfterEach
  void tearDown() {
    instanceDao.dropSchema(INSTANCE);
  }

  @Test
  void updateSingleRecord() {
    String newVal = "val2";

    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    // Overwrite the value for the only attribute on the record
    RecordRequest updateRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(TEST_KEY, newVal));

    RecordResponse resp =
        recordOrchestratorService.updateSingleRecord(
            INSTANCE, VERSION, TEST_TYPE, RECORD_ID, updateRequest);

    assertEquals(RECORD_ID, resp.recordId());

    // Check that we now get the new val for the attribute
    testGetRecord(RECORD_ID, TEST_KEY, newVal);
  }

  @Test
  void queryForRecords() {
    String secondRecord = "r2";
    String secondVal = "v2";
    String thirdRecord = "r3";
    String thirdVal = "v3";

    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);
    testCreateRecord(secondRecord, TEST_KEY, secondVal);
    testCreateRecord(thirdRecord, TEST_KEY, thirdVal);

    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            INSTANCE, TEST_TYPE, VERSION, new SearchRequest());

    testContainsRecord(RECORD_ID, TEST_KEY, TEST_VAL, resp.records());
    testContainsRecord(secondRecord, TEST_KEY, secondVal, resp.records());
    testContainsRecord(thirdRecord, TEST_KEY, thirdVal, resp.records());
    assertEquals(3, resp.totalRecords());
  }

  @Test
  void upsertSingleRecord() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    testGetRecord(RECORD_ID, TEST_KEY, TEST_VAL);
  }

  @Test
  void deleteSingleRecord() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    recordOrchestratorService.deleteSingleRecord(INSTANCE, VERSION, TEST_TYPE, RECORD_ID);

    assertThrows(
        MissingObjectException.class,
        () -> testGetRecord(RECORD_ID, TEST_KEY, TEST_VAL),
        "getRecord should have thrown an error");
  }

  @Test
  void deleteRecordType() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    recordOrchestratorService.deleteRecordType(INSTANCE, VERSION, TEST_TYPE);

    assertThrows(
        MissingObjectException.class,
        () -> recordOrchestratorService.describeRecordType(INSTANCE, VERSION, TEST_TYPE),
        "describeRecordType should have thrown an error");
  }

  @Test
  void deleteAttribute() {
    // Arrange
    setUpDeleteAttributeTest();

    // Act
    recordOrchestratorService.deleteAttribute(INSTANCE, VERSION, TEST_TYPE, "attr2");

    // Assert
    assertAttributes(Set.of("id", "attr1"));
  }

  @Test
  void deletePrimaryKeyAttribute() {
    // Arrange
    setUpDeleteAttributeTest();

    // Act/Assert
    assertThrows(
        DeleteAttributeRequestException.class,
        () -> recordOrchestratorService.deleteAttribute(INSTANCE, VERSION, TEST_TYPE, "id"),
        "Unable to delete ID attribute");
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  @Test
  void deleteNonexistentAttribute() {
    // Arrange
    setUpDeleteAttributeTest();

    // Act/Assert
    assertThrows(
        MissingObjectException.class,
        () ->
            recordOrchestratorService.deleteAttribute(INSTANCE, VERSION, TEST_TYPE, "doesnotexist"),
        "Attribute not found");
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  private void setUpDeleteAttributeTest() {
    RecordRequest recordRequest =
        new RecordRequest(
            RecordAttributes.empty().putAttribute("attr1", "foo").putAttribute("attr2", "bar"));

    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            INSTANCE, VERSION, TEST_TYPE, "1", Optional.of("id"), recordRequest);
  }

  private void assertAttributes(Set<String> expectedAttributeNames) {
    RecordTypeSchema schema =
        recordOrchestratorService.describeRecordType(INSTANCE, VERSION, TEST_TYPE);
    Set<String> actualAttributeNames =
        Set.copyOf(schema.attributes().stream().map(AttributeSchema::name).toList());
    assertEquals(actualAttributeNames, expectedAttributeNames);
  }

  @Test
  void describeRecordType() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);
    testCreateRecord("second", TEST_KEY, "another");
    testCreateRecord("third", TEST_KEY, "a third");

    RecordTypeSchema schema =
        recordOrchestratorService.describeRecordType(INSTANCE, VERSION, TEST_TYPE);

    assertEquals(TEST_TYPE, schema.name());
    assertEquals(3, schema.count());
  }

  @Test
  void describeAllRecordTypes() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);
    testCreateRecord("second", TEST_KEY, "another");
    testCreateRecord("third", TEST_KEY, "a third");

    RecordType typeTwo = RecordType.valueOf("typeTwo");
    testCreateRecord("fourth", TEST_KEY, "a fourth", typeTwo);

    List<RecordTypeSchema> schemas =
        recordOrchestratorService.describeAllRecordTypes(INSTANCE, VERSION);

    assert (schemas.stream()
        .anyMatch(schema -> schema.name().equals(TEST_TYPE) && schema.count() == 3));
    assert (schemas.stream()
        .anyMatch(schema -> schema.name().equals(typeTwo) && schema.count() == 1));
    assertEquals(2, schemas.size());
  }

  @Test
  void testValidateVersion() {
    validateVersion(VERSION);

    ResponseStatusException e =
        assertThrows(
            ResponseStatusException.class,
            () -> validateVersion("invalidVersion"),
            "validateVersion should have thrown an error");
    assert (e.getStatusCode().equals(HttpStatus.BAD_REQUEST));
  }

  private void testCreateRecord(String newRecordId, String testKey, String testVal) {
    testCreateRecord(newRecordId, testKey, testVal, TEST_TYPE);
  }

  private void testCreateRecord(
      String newRecordId, String testKey, String testVal, RecordType newRecordType) {
    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(testKey, testVal));

    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            INSTANCE, VERSION, newRecordType, newRecordId, Optional.empty(), recordRequest);

    assertEquals(newRecordId, response.getBody().recordId());
  }

  private void testGetRecord(String newRecordId, String testKey, String testVal) {
    RecordResponse recordResponse =
        recordOrchestratorService.getSingleRecord(INSTANCE, VERSION, TEST_TYPE, newRecordId);
    assertEquals(testVal, recordResponse.recordAttributes().getAttributeValue(testKey));
  }

  private void testContainsRecord(
      String recordId, String testKey, String testVal, List<RecordResponse> respList) {
    boolean found =
        respList.stream()
            .anyMatch(
                recordResponse ->
                    recordResponse.recordId().equals(recordId)
                        && recordResponse
                            .recordAttributes()
                            .getAttributeValue(testKey)
                            .equals(testVal));
    assert (found);
  }
}
