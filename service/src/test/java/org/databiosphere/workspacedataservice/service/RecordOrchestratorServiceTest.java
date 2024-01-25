package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
  private static final String PRIMARY_KEY = "id";
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
  void renameAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act
    recordOrchestratorService.renameAttribute(INSTANCE, VERSION, TEST_TYPE, "attr2", "attr3");

    // Assert
    assertAttributes(Set.of("id", "attr1", "attr3"));
    testGetRecord(RECORD_ID, "attr3", "attr2");
  }

  @Test
  void renamePrimaryKeyAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act/Assert
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                recordOrchestratorService.renameAttribute(
                    INSTANCE, VERSION, TEST_TYPE, "id", "newId"),
            "renameAttribute should have thrown an error");
    assertEquals("Unable to rename primary key attribute", e.getMessage());
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  @Test
  void renameNonexistentAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act/Assert
    MissingObjectException e =
        assertThrows(
            MissingObjectException.class,
            () ->
                recordOrchestratorService.renameAttribute(
                    INSTANCE, VERSION, TEST_TYPE, "doesNotExist", "attr3"),
            "renameAttribute should have thrown an error");
    assertEquals("Attribute does not exist", e.getMessage());
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  @Test
  void renameAttributeConflictingName() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act/Assert
    ConflictException e =
        assertThrows(
            ConflictException.class,
            () ->
                recordOrchestratorService.renameAttribute(
                    INSTANCE, VERSION, TEST_TYPE, "attr1", "attr2"),
            "renameAttribute should have thrown an error");
    assertEquals("Attribute already exists", e.getMessage());
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  @ParameterizedTest(name = "update attribute data type from {1} to {2}")
  @MethodSource({
    "org.databiosphere.workspacedataservice.DataTypeConversionTestCases#stringConversions",
    "org.databiosphere.workspacedataservice.DataTypeConversionTestCases#stringArrayConversions",
    "org.databiosphere.workspacedataservice.DataTypeConversionTestCases#numberConversions",
    "org.databiosphere.workspacedataservice.DataTypeConversionTestCases#numberArrayConversions",
    "org.databiosphere.workspacedataservice.DataTypeConversionTestCases#booleanConversions",
    "org.databiosphere.workspacedataservice.DataTypeConversionTestCases#booleanArrayConversions"
  })
  void updateAttributeDataType(
      Object attributeValue,
      DataTypeMapping expectedInitialDataType,
      DataTypeMapping newDataType,
      Object expectedFinalAttributeValue) {
    // Arrange
    String attributeName = "testAttribute";
    RecordAttributes recordAttributes =
        RecordAttributes.empty().putAttribute(attributeName, attributeValue);
    RecordRequest recordRequest = new RecordRequest(recordAttributes);
    recordOrchestratorService.upsertSingleRecord(
        INSTANCE, VERSION, TEST_TYPE, RECORD_ID, Optional.of(PRIMARY_KEY), recordRequest);

    assertAttributeDataType(
        attributeName,
        expectedInitialDataType,
        "expected initial attribute data type to be %s".formatted(expectedInitialDataType));

    // Act
    recordOrchestratorService.updateAttributeDataType(
        INSTANCE, VERSION, TEST_TYPE, attributeName, newDataType.name());

    // Assert
    assertAttributeDataType(
        attributeName,
        newDataType,
        "expected attribute data type to be updated to %s".formatted(newDataType));
    assertAttributeValue(
        RECORD_ID,
        attributeName,
        expectedFinalAttributeValue,
        "expected %s %s to be converted to %s %s"
            .formatted(
                expectedInitialDataType, attributeValue, newDataType, expectedFinalAttributeValue));
  }

  @Test
  void updateAttributeDataTypePrimaryKey() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1"});

    // Act/Assert
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    INSTANCE, VERSION, TEST_TYPE, PRIMARY_KEY, "NUMBER"),
            "updateAttributeDataType should have thrown an error");
    assertEquals("Unable to update primary key attribute", e.getMessage());
  }

  @Test
  void updateAttributeDataTypeInvalidDataType() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1"});

    // Act/Assert
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    INSTANCE, VERSION, TEST_TYPE, "attr1", "INVALID_DATA_TYPE"),
            "updateAttributeDataType should have thrown an error");
    assertEquals("Invalid datatype", e.getMessage());
  }

  @Test
  void updateAttributeDataTypeUnableToConvertValues() {
    // Arrange
    String attributeName = "testAttribute";

    RecordAttributes recordOneAttributes =
        RecordAttributes.empty().putAttribute(attributeName, "123");
    RecordRequest recordOneRequest = new RecordRequest(recordOneAttributes);
    recordOrchestratorService.upsertSingleRecord(
        INSTANCE, VERSION, TEST_TYPE, "row_1", Optional.of(PRIMARY_KEY), recordOneRequest);

    RecordAttributes recordTwoAttributes =
        RecordAttributes.empty().putAttribute(attributeName, "foo");
    RecordRequest recordTwoRequest = new RecordRequest(recordTwoAttributes);
    recordOrchestratorService.upsertSingleRecord(
        INSTANCE, VERSION, TEST_TYPE, "row_2", Optional.of(PRIMARY_KEY), recordTwoRequest);

    assertAttributeDataType(
        attributeName, DataTypeMapping.STRING, "expected initial attribute data type to be STRING");

    // Act/Assert
    ConflictException e =
        assertThrows(
            ConflictException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    INSTANCE, VERSION, TEST_TYPE, attributeName, "NUMBER"),
            "updateAttributeDataType should have thrown an error");

    assertEquals(
        "Unable to convert values for attribute %s to NUMBER".formatted(attributeName),
        e.getMessage());

    assertAttributeDataType(
        attributeName, DataTypeMapping.STRING, "expected attribute data type to be unchanged");
    assertAttributeValue(
        "row_1", attributeName, "123", "expected attribute values to be unchanged");
    assertAttributeValue(
        "row_2", attributeName, "foo", "expected attribute values to be unchanged");
  }

  @Test
  void deleteAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act
    recordOrchestratorService.deleteAttribute(INSTANCE, VERSION, TEST_TYPE, "attr2");

    // Assert
    assertAttributes(Set.of("id", "attr1"));
  }

  @Test
  void deletePrimaryKeyAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act/Assert
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> recordOrchestratorService.deleteAttribute(INSTANCE, VERSION, TEST_TYPE, "id"),
            "deleteAttribute should have thrown an error");
    assertEquals("Unable to delete primary key attribute", e.getMessage());
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  @Test
  void deleteNonexistentAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act/Assert
    MissingObjectException e =
        assertThrows(
            MissingObjectException.class,
            () ->
                recordOrchestratorService.deleteAttribute(
                    INSTANCE, VERSION, TEST_TYPE, "doesnotexist"),
            "deleteAttribute should have thrown an error");
    assertEquals("Attribute does not exist", e.getMessage());
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  private void createRecordWithAttributes(String[] attributeNames) {
    RecordAttributes recordAttributes = RecordAttributes.empty();
    for (String attribute : attributeNames) {
      recordAttributes.putAttribute(attribute, attribute);
    }
    RecordRequest recordRequest = new RecordRequest(recordAttributes);

    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            INSTANCE, VERSION, TEST_TYPE, RECORD_ID, Optional.of(PRIMARY_KEY), recordRequest);

    Set<String> expectedAttributes = Stream.of(attributeNames).collect(Collectors.toSet());
    expectedAttributes.add(PRIMARY_KEY);

    assertAttributes(expectedAttributes);
  }

  private void assertAttributes(Set<String> expectedAttributeNames) {
    RecordTypeSchema schema =
        recordOrchestratorService.describeRecordType(INSTANCE, VERSION, TEST_TYPE);
    Set<String> actualAttributeNames =
        Set.copyOf(schema.attributes().stream().map(AttributeSchema::name).toList());
    assertEquals(expectedAttributeNames, actualAttributeNames);
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

  private void assertAttributeDataType(
      String attributeName, DataTypeMapping dataType, String message) {
    RecordTypeSchema recordTypeSchema =
        recordOrchestratorService.describeRecordType(INSTANCE, VERSION, TEST_TYPE);
    AttributeSchema attributeSchema = recordTypeSchema.getAttributeSchema(attributeName);
    assertEquals(dataType.name(), attributeSchema.datatype(), message);
  }

  private void assertAttributeValue(
      String recordId, String attributeName, Object expectedValue, String message) {
    RecordResponse record =
        recordOrchestratorService.getSingleRecord(INSTANCE, VERSION, TEST_TYPE, recordId);
    Object attributeValue = record.recordAttributes().getAttributeValue(attributeName);

    if (expectedValue instanceof Object[]) {
      assertArrayEquals((Object[]) expectedValue, (Object[]) attributeValue, message);
    } else {
      assertEquals(expectedValue, attributeValue, message);
    }
  }
}
