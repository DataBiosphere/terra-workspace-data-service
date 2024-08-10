package org.databiosphere.workspacedataservice.service;

import static java.util.UUID.randomUUID;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictingPrimaryKeysException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.databiosphere.workspacedataservice.shared.model.SortDirection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

@SpringBootTest
class RecordOrchestratorServiceTest extends TestBase {

  @Autowired private CollectionDao collectionDao;
  @Autowired private RecordOrchestratorService recordOrchestratorService;

  private static final UUID COLLECTION_UUID = randomUUID();
  private static final CollectionId COLLECTION_ID = CollectionId.of(COLLECTION_UUID);
  private static final RecordType TEST_TYPE = RecordType.valueOf("test");

  private static final String RECORD_ID = "aNewRecord";
  private static final String PRIMARY_KEY = "id";
  private static final String TEST_KEY = "test_key";
  private static final String TEST_VAL = "val";

  @BeforeEach
  void setUp() {
    if (!collectionDao.collectionSchemaExists(COLLECTION_ID)) {
      collectionDao.createSchema(COLLECTION_ID);
    }
  }

  @AfterEach
  void tearDown() {
    collectionDao.dropSchema(COLLECTION_ID);
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
            COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, updateRequest);

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
            COLLECTION_UUID, TEST_TYPE, VERSION, new SearchRequest());

    testContainsRecord(RECORD_ID, TEST_KEY, TEST_VAL, resp.records());
    testContainsRecord(secondRecord, TEST_KEY, secondVal, resp.records());
    testContainsRecord(thirdRecord, TEST_KEY, thirdVal, resp.records());
    assertEquals(3, resp.totalRecords());
  }

  @Test
  void sortAscending() {
    // insert records out of any order to ensure native db order doesn't give false positives
    testCreateRecord("two", TEST_KEY, "value2");
    testCreateRecord("one", TEST_KEY, "value1");
    testCreateRecord("three", TEST_KEY, "value3");

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setSortAttribute(TEST_KEY);
    searchRequest.setSort(SortDirection.ASC);

    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            COLLECTION_UUID, TEST_TYPE, VERSION, searchRequest);

    assertEquals(3, resp.totalRecords());

    // extract actual ids, in order, from the response
    List<String> actualIds = resp.records().stream().map(RecordResponse::recordId).toList();
    assertEquals(List.of("one", "two", "three"), actualIds);
  }

  @Test
  void sortDescending() {
    // insert records out of any order to ensure native db order doesn't give false positives
    testCreateRecord("two", TEST_KEY, "value2");
    testCreateRecord("one", TEST_KEY, "value1");
    testCreateRecord("three", TEST_KEY, "value3");

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setSortAttribute(TEST_KEY);
    searchRequest.setSort(SortDirection.DESC);

    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            COLLECTION_UUID, TEST_TYPE, VERSION, searchRequest);

    assertEquals(3, resp.totalRecords());

    // extract actual ids, in order, from the response
    List<String> actualIds = resp.records().stream().map(RecordResponse::recordId).toList();
    assertEquals(List.of("three", "two", "one"), actualIds);
  }

  @Test
  void sortPrimaryKeyImplicit() {
    // insert records out of any order to ensure native db order doesn't give false positives
    testCreateRecord("two", TEST_KEY, "value2");
    testCreateRecord("one", TEST_KEY, "value1");
    testCreateRecord("three", TEST_KEY, "value3");

    SearchRequest searchRequest = new SearchRequest();
    // omitting the sort attribute will implicitly sort by primary key
    searchRequest.setSort(SortDirection.DESC);

    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            COLLECTION_UUID, TEST_TYPE, VERSION, searchRequest);

    assertEquals(3, resp.totalRecords());

    // extract actual ids, in order, from the response
    List<String> actualIds = resp.records().stream().map(RecordResponse::recordId).toList();
    assertEquals(List.of("two", "three", "one"), actualIds); // descending alpha sort on pk
  }

  @Test
  void sortPrimaryKeyExplicit() {
    // insert records out of any order to ensure native db order doesn't give false positives
    testCreateRecord("two", TEST_KEY, "value2");
    testCreateRecord("one", TEST_KEY, "value1");
    testCreateRecord("three", TEST_KEY, "value3");

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setSortAttribute("sys_name"); // default primary key name for testCreateRecord()
    searchRequest.setSort(SortDirection.DESC);

    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            COLLECTION_UUID, TEST_TYPE, VERSION, searchRequest);

    assertEquals(3, resp.totalRecords());

    // extract actual ids, in order, from the response
    List<String> actualIds = resp.records().stream().map(RecordResponse::recordId).toList();
    assertEquals(List.of("two", "three", "one"), actualIds); // descending alpha sort on pk
  }

  @Test
  void upsertSingleRecord() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    testGetRecord(RECORD_ID, TEST_KEY, TEST_VAL);
  }

  @Test
  void upsertNewRecordWithMatchingPrimaryKey() {
    // Arrange
    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(PRIMARY_KEY, RECORD_ID));

    // Act
    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            COLLECTION_UUID,
            VERSION,
            TEST_TYPE,
            RECORD_ID,
            Optional.of(PRIMARY_KEY),
            recordRequest);

    // Assert
    assertEquals(HttpStatus.CREATED, response.getStatusCode());
  }

  @Test
  void upsertNewRecordWithDifferentPrimaryKey() {
    // Arrange
    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(PRIMARY_KEY, "someOtherValue"));

    // Act/Assert
    Optional<String> primaryKey = Optional.of(PRIMARY_KEY);
    assertThrows(
        ConflictingPrimaryKeysException.class,
        () ->
            recordOrchestratorService.upsertSingleRecord(
                COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, primaryKey, recordRequest),
        "upsertSingleRecord should have thrown an error");
  }

  @Test
  void upsertNewRecordWithMatchingDefaultPrimaryKey() {
    // Arrange
    RecordRequest recordRequest =
        new RecordRequest(
            RecordAttributes.empty().putAttribute(ReservedNames.RECORD_ID, RECORD_ID));

    // Act
    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, Optional.empty(), recordRequest);

    // Assert
    assertEquals(HttpStatus.CREATED, response.getStatusCode());
  }

  @Test
  void upsertNewRecordWithDifferentDefaultPrimaryKey() {
    // Arrange
    RecordRequest recordRequest =
        new RecordRequest(
            RecordAttributes.empty().putAttribute(ReservedNames.RECORD_ID, "someOtherValue"));

    // Act/Assert
    assertThrows(
        ConflictingPrimaryKeysException.class,
        () ->
            recordOrchestratorService.upsertSingleRecord(
                COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, Optional.empty(), recordRequest),
        "upsertSingleRecord should have thrown an error");
  }

  @Test
  void upsertExistingRecordWithMatchingPrimaryKey() {
    // Arrange
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID,
        VERSION,
        TEST_TYPE,
        RECORD_ID,
        Optional.of(PRIMARY_KEY),
        new RecordRequest(RecordAttributes.empty()));

    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(PRIMARY_KEY, RECORD_ID));

    // Act
    Optional<String> primaryKey = Optional.of(PRIMARY_KEY);
    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, primaryKey, recordRequest);

    // Assert
    assertEquals(HttpStatus.OK, response.getStatusCode());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void upsertExistingRecordWithDifferentPrimaryKey(boolean primaryKeyInQuery) {
    // Arrange
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID,
        VERSION,
        TEST_TYPE,
        RECORD_ID,
        Optional.of(PRIMARY_KEY),
        new RecordRequest(RecordAttributes.empty()));

    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(PRIMARY_KEY, "someOtherValue"));

    // Act/Assert
    Optional<String> primaryKey = primaryKeyInQuery ? Optional.of(PRIMARY_KEY) : Optional.empty();
    assertThrows(
        ConflictingPrimaryKeysException.class,
        () ->
            recordOrchestratorService.upsertSingleRecord(
                COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, primaryKey, recordRequest),
        "upsertSingleRecord should have thrown an error");
  }

  @Test
  void updateRecordWithMatchingPrimaryKey() {
    // Arrange
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID,
        VERSION,
        TEST_TYPE,
        RECORD_ID,
        Optional.of(PRIMARY_KEY),
        new RecordRequest(RecordAttributes.empty()));

    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(PRIMARY_KEY, RECORD_ID));

    // Act
    RecordResponse response =
        recordOrchestratorService.updateSingleRecord(
            COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, recordRequest);

    // Assert
    assertEquals(RECORD_ID, response.recordId());
  }

  @Test
  void updateRecordWithDifferentPrimaryKey() {
    // Arrange
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID,
        VERSION,
        TEST_TYPE,
        RECORD_ID,
        Optional.of(PRIMARY_KEY),
        new RecordRequest(RecordAttributes.empty()));

    RecordRequest recordRequest =
        new RecordRequest(RecordAttributes.empty().putAttribute(PRIMARY_KEY, "someOtherValue"));

    // Act/Assert
    assertThrows(
        ConflictingPrimaryKeysException.class,
        () ->
            recordOrchestratorService.updateSingleRecord(
                COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, recordRequest),
        "updateSingleRecord should have thrown an error");
  }

  @Test
  void deleteSingleRecord() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    recordOrchestratorService.deleteSingleRecord(COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID);

    assertThrows(
        MissingObjectException.class,
        () -> testGetRecord(RECORD_ID, TEST_KEY, TEST_VAL),
        "getRecord should have thrown an error");
  }

  @Test
  void deleteRecordType() {
    testCreateRecord(RECORD_ID, TEST_KEY, TEST_VAL);

    recordOrchestratorService.deleteRecordType(COLLECTION_UUID, VERSION, TEST_TYPE);

    assertThrows(
        MissingObjectException.class,
        () -> recordOrchestratorService.describeRecordType(COLLECTION_UUID, VERSION, TEST_TYPE),
        "describeRecordType should have thrown an error");
  }

  @Test
  void renameAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act
    recordOrchestratorService.renameAttribute(
        COLLECTION_UUID, VERSION, TEST_TYPE, "attr2", "attr3");

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
                    COLLECTION_UUID, VERSION, TEST_TYPE, "id", "newId"),
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
                    COLLECTION_UUID, VERSION, TEST_TYPE, "doesNotExist", "attr3"),
            "renameAttribute should have thrown an error");
    assertEquals(
        "Attribute does not exist or you do not have permission to see it", e.getMessage());
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
                    COLLECTION_UUID, VERSION, TEST_TYPE, "attr1", "attr2"),
            "renameAttribute should have thrown an error");
    assertEquals("Attribute already exists", e.getMessage());
    assertAttributes(Set.of("id", "attr1", "attr2"));
  }

  @ParameterizedTest(name = "update attribute data type from {1} to {2}")
  @MethodSource({
    "updateAttributeDataTypeStringConversions",
    "updateAttributeDataTypeStringArrayConversions",
    "updateAttributeDataTypeNumberConversions",
    "updateAttributeDataTypeNumberArrayConversions",
    "updateAttributeDataTypeBooleanConversions",
    "updateAttributeDataTypeBooleanArrayConversions",
    "updateAttributeDataTypeDateConversions",
    "updateAttributeDataTypeDateArrayConversions",
    "updateAttributeDataTypeDatetimeConversions",
    "updateAttributeDataTypeDatetimeArrayConversions"
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
        COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, Optional.of(PRIMARY_KEY), recordRequest);

    assertAttributeDataType(
        attributeName,
        expectedInitialDataType,
        "expected initial attribute data type to be %s".formatted(expectedInitialDataType));

    // Act
    recordOrchestratorService.updateAttributeDataType(
        COLLECTION_UUID, VERSION, TEST_TYPE, attributeName, newDataType.name());

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

  static Stream<Arguments> updateAttributeDataTypeStringConversions() {
    return Stream.of(
        // Number
        Arguments.of(
            "123", DataTypeMapping.STRING, DataTypeMapping.NUMBER, BigDecimal.valueOf(123)),
        // Boolean
        Arguments.of("yes", DataTypeMapping.STRING, DataTypeMapping.BOOLEAN, Boolean.TRUE),
        Arguments.of("no", DataTypeMapping.STRING, DataTypeMapping.BOOLEAN, Boolean.FALSE),
        // Date
        Arguments.of(
            "2024/01/24", DataTypeMapping.STRING, DataTypeMapping.DATE, LocalDate.of(2024, 1, 24)),
        // Datetime
        Arguments.of(
            "2024/01/24 02:30:00",
            DataTypeMapping.STRING,
            DataTypeMapping.DATE_TIME,
            LocalDateTime.of(2024, 1, 24, 2, 30, 0)),
        // String array
        Arguments.of(
            "foo", DataTypeMapping.STRING, DataTypeMapping.ARRAY_OF_STRING, new String[] {"foo"}),
        // Number array
        Arguments.of(
            "123",
            DataTypeMapping.STRING,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(123)}),
        // Boolean array
        Arguments.of(
            "yes",
            DataTypeMapping.STRING,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE}),
        // Date array
        Arguments.of(
            "2024/01/24",
            DataTypeMapping.STRING,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}),
        // Datetime array
        Arguments.of(
            "2024/01/24 02:30:00",
            DataTypeMapping.STRING,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 2, 30, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeStringArrayConversions() {
    return Stream.of(
        // Number array
        Arguments.of(
            List.of("123"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(123)}),
        // Boolean array
        Arguments.of(
            List.of("yes", "no"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE, Boolean.FALSE}),
        // Date array
        Arguments.of(
            List.of("2024/01/24"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}),
        // Datetime array
        Arguments.of(
            List.of("2024/01/24 02:30:00"),
            DataTypeMapping.ARRAY_OF_STRING,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 2, 30, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeNumberConversions() {
    return Stream.of(
        // String
        Arguments.of(
            BigDecimal.valueOf(123), DataTypeMapping.NUMBER, DataTypeMapping.STRING, "123"),
        // Boolean
        Arguments.of(
            BigDecimal.valueOf(1), DataTypeMapping.NUMBER, DataTypeMapping.BOOLEAN, Boolean.TRUE),
        Arguments.of(
            BigDecimal.valueOf(0), DataTypeMapping.NUMBER, DataTypeMapping.BOOLEAN, Boolean.FALSE),
        // Date
        Arguments.of(
            BigDecimal.valueOf(1706063400L),
            DataTypeMapping.NUMBER,
            DataTypeMapping.DATE,
            LocalDate.of(2024, 1, 24)),
        // Datetime
        Arguments.of(
            BigDecimal.valueOf(1706063400L),
            DataTypeMapping.NUMBER,
            DataTypeMapping.DATE_TIME,
            LocalDateTime.of(2024, 1, 24, 2, 30, 0)),
        // String array
        Arguments.of(
            BigDecimal.valueOf(123),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"123"}),
        // Number array
        Arguments.of(
            BigDecimal.valueOf(123),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(123)}),
        // Boolean array
        Arguments.of(
            BigDecimal.valueOf(1),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE}),
        // Date array
        Arguments.of(
            BigDecimal.valueOf(1706063400L),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}),
        // Datetime array
        Arguments.of(
            BigDecimal.valueOf(1706063400L),
            DataTypeMapping.NUMBER,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 2, 30, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeNumberArrayConversions() {
    return Stream.of(
        // String array
        Arguments.of(
            List.of(BigDecimal.valueOf(123)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"123"}),
        // Boolean array
        Arguments.of(
            List.of(BigDecimal.valueOf(1), BigDecimal.valueOf(0)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE, Boolean.FALSE}),
        // Date array
        Arguments.of(
            List.of(BigDecimal.valueOf(1706063400L)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}),
        // Datetime array
        Arguments.of(
            List.of(BigDecimal.valueOf(1706063400L)),
            DataTypeMapping.ARRAY_OF_NUMBER,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 2, 30, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeBooleanConversions() {
    return Stream.of(
        // String
        Arguments.of(Boolean.TRUE, DataTypeMapping.BOOLEAN, DataTypeMapping.STRING, "true"),
        Arguments.of(Boolean.FALSE, DataTypeMapping.BOOLEAN, DataTypeMapping.STRING, "false"),
        // Number
        Arguments.of(
            Boolean.TRUE, DataTypeMapping.BOOLEAN, DataTypeMapping.NUMBER, BigDecimal.valueOf(1)),
        Arguments.of(
            Boolean.FALSE, DataTypeMapping.BOOLEAN, DataTypeMapping.NUMBER, BigDecimal.valueOf(0)),
        // String array
        Arguments.of(
            Boolean.TRUE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"true"}),
        Arguments.of(
            Boolean.FALSE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"false"}),
        // Number array
        Arguments.of(
            Boolean.TRUE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(1)}),
        Arguments.of(
            Boolean.FALSE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(0)}),
        // Boolean array
        Arguments.of(
            Boolean.TRUE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.TRUE}),
        Arguments.of(
            Boolean.FALSE,
            DataTypeMapping.BOOLEAN,
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            new Boolean[] {Boolean.FALSE}));
  }

  static Stream<Arguments> updateAttributeDataTypeBooleanArrayConversions() {
    return Stream.of(
        // String array
        Arguments.of(
            List.of(Boolean.TRUE, Boolean.FALSE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"true", "false"}),
        // Number array
        Arguments.of(
            List.of(Boolean.TRUE, Boolean.FALSE),
            DataTypeMapping.ARRAY_OF_BOOLEAN,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(1), BigDecimal.valueOf(0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeDateConversions() {
    return Stream.of(
        // String
        Arguments.of(
            LocalDate.of(2024, 1, 24), DataTypeMapping.DATE, DataTypeMapping.STRING, "2024-01-24"),
        // Number
        Arguments.of(
            LocalDate.of(2024, 1, 24),
            DataTypeMapping.DATE,
            DataTypeMapping.NUMBER,
            BigDecimal.valueOf(1706054400L)),
        // Datetime
        Arguments.of(
            LocalDate.of(2024, 1, 24),
            DataTypeMapping.DATE,
            DataTypeMapping.DATE_TIME,
            LocalDateTime.of(2024, 1, 24, 0, 0, 0)),
        // String array
        Arguments.of(
            LocalDate.of(2024, 1, 24),
            DataTypeMapping.DATE,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"2024-01-24"}),
        // Number array
        Arguments.of(
            LocalDate.of(2024, 1, 24),
            DataTypeMapping.DATE,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(1706054400L)}),
        // Date array
        Arguments.of(
            LocalDate.of(2024, 1, 24),
            DataTypeMapping.DATE,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}),
        // Datetime array
        Arguments.of(
            LocalDate.of(2024, 1, 24),
            DataTypeMapping.DATE,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 0, 0, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeDateArrayConversions() {
    return Stream.of(
        // String array
        Arguments.of(
            List.of(LocalDate.of(2024, 1, 24)),
            DataTypeMapping.ARRAY_OF_DATE,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"2024-01-24"}),
        // Number array
        Arguments.of(
            List.of(LocalDate.of(2024, 1, 24)),
            DataTypeMapping.ARRAY_OF_DATE,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {BigDecimal.valueOf(1706054400L)}),
        // Datetime array
        Arguments.of(
            List.of(LocalDate.of(2024, 1, 24)),
            DataTypeMapping.ARRAY_OF_DATE,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 0, 0, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeDatetimeConversions() {
    return Stream.of(
        // String
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.STRING,
            "2024-01-24 02:30:00+00"),
        // Number
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.NUMBER,
            new BigDecimal("1706063400.000000")),
        // Datetime
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.DATE,
            LocalDate.of(2024, 1, 24)),
        // String array
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"2024-01-24 02:30:00+00"}),
        // Number array
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {new BigDecimal("1706063400.000000")}),
        // Date array
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}),
        // Datetime array
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            new LocalDateTime[] {LocalDateTime.of(2024, 1, 24, 2, 30, 0)}));
  }

  static Stream<Arguments> updateAttributeDataTypeDatetimeArrayConversions() {
    return Stream.of(
        // String array
        Arguments.of(
            List.of(LocalDateTime.of(2024, 1, 24, 2, 30, 0)),
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            DataTypeMapping.ARRAY_OF_STRING,
            new String[] {"2024-01-24 02:30:00+00"}),
        // Number array
        Arguments.of(
            List.of(LocalDateTime.of(2024, 1, 24, 2, 30, 0)),
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            DataTypeMapping.ARRAY_OF_NUMBER,
            new BigDecimal[] {new BigDecimal("1706063400.000000")}),
        // Date array
        Arguments.of(
            List.of(LocalDateTime.of(2024, 1, 24, 2, 30, 0)),
            DataTypeMapping.ARRAY_OF_DATE_TIME,
            DataTypeMapping.ARRAY_OF_DATE,
            new LocalDate[] {LocalDate.of(2024, 1, 24)}));
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
                    COLLECTION_UUID, VERSION, TEST_TYPE, PRIMARY_KEY, "NUMBER"),
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
                    COLLECTION_UUID, VERSION, TEST_TYPE, "attr1", "INVALID_DATA_TYPE"),
            "updateAttributeDataType should have thrown an error");
    assertEquals("Invalid datatype", e.getMessage());
  }

  @Test
  void updateAttributeDataTypeArrayToScalar() {
    // Arrange
    String attributeName = "testAttribute";
    RecordAttributes recordAttributes =
        RecordAttributes.empty().putAttribute(attributeName, List.of("foo", "bar", "baz"));
    RecordRequest recordRequest = new RecordRequest(recordAttributes);
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, Optional.of(PRIMARY_KEY), recordRequest);

    assertAttributeDataType(
        attributeName,
        DataTypeMapping.ARRAY_OF_STRING,
        "expected initial attribute data type to be ARRAY_OF_STRING");

    // Act/Assert
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    COLLECTION_UUID, VERSION, TEST_TYPE, attributeName, "STRING"),
            "updateAttributeDataType should have thrown an error");
    assertEquals("Unable to convert array type to scalar type", e.getMessage());
  }

  @Test
  void updateAttributeDataTypeFailureToConvertValueChangesNoData() {
    // Arrange
    String attributeName = "testAttribute";

    RecordAttributes recordOneAttributes =
        RecordAttributes.empty().putAttribute(attributeName, "123");
    RecordRequest recordOneRequest = new RecordRequest(recordOneAttributes);
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID, VERSION, TEST_TYPE, "row_1", Optional.of(PRIMARY_KEY), recordOneRequest);

    RecordAttributes recordTwoAttributes =
        RecordAttributes.empty().putAttribute(attributeName, "foo");
    RecordRequest recordTwoRequest = new RecordRequest(recordTwoAttributes);
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID, VERSION, TEST_TYPE, "row_2", Optional.of(PRIMARY_KEY), recordTwoRequest);

    assertAttributeDataType(
        attributeName, DataTypeMapping.STRING, "expected initial attribute data type to be STRING");

    // Act/Assert
    ConflictException e =
        assertThrows(
            ConflictException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    COLLECTION_UUID, VERSION, TEST_TYPE, attributeName, "NUMBER"),
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

  @ParameterizedTest(name = "gracefully fails to convert {0} to {1}")
  @MethodSource("unableToConvertValuesTestCases")
  void updateAttributeDataTypeUnableToConvertValues(
      Object attributeValue, DataTypeMapping newDataType) {
    // Arrange
    String attributeName = "testAttribute";

    RecordAttributes recordAttributes =
        RecordAttributes.empty().putAttribute(attributeName, attributeValue);
    RecordRequest recordRequest = new RecordRequest(recordAttributes);
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, Optional.of(PRIMARY_KEY), recordRequest);

    // Act/Assert
    ConflictException e =
        assertThrows(
            ConflictException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    COLLECTION_UUID, VERSION, TEST_TYPE, attributeName, newDataType.name()),
            "updateAttributeDataType should have thrown an error");

    assertEquals(
        "Unable to convert values for attribute %s to %s".formatted(attributeName, newDataType),
        e.getMessage());
  }

  static Stream<Arguments> unableToConvertValuesTestCases() {
    return Stream.of(
        Arguments.of("foo", DataTypeMapping.NUMBER),
        Arguments.of(1000000000000000L, DataTypeMapping.DATE_TIME));
  }

  @ParameterizedTest(name = "rejects requests to convert from {1} to {2}")
  @MethodSource("invalidConversionTestCases")
  void updateAttributeDataTypeInvalidConversion(
      Object attributeValue, DataTypeMapping expectedInitialDataType, DataTypeMapping newDataType) {
    // Arrange
    String attributeName = "testAttribute";

    RecordAttributes recordAttributes =
        RecordAttributes.empty().putAttribute(attributeName, attributeValue);
    RecordRequest recordRequest = new RecordRequest(recordAttributes);
    recordOrchestratorService.upsertSingleRecord(
        COLLECTION_UUID, VERSION, TEST_TYPE, RECORD_ID, Optional.of(PRIMARY_KEY), recordRequest);
    assertAttributeDataType(
        attributeName,
        expectedInitialDataType,
        "expected initial attribute data type to be %s".formatted(expectedInitialDataType));

    // Act/Assert
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                recordOrchestratorService.updateAttributeDataType(
                    COLLECTION_UUID, VERSION, TEST_TYPE, attributeName, newDataType.name()),
            "updateAttributeDataType should have thrown an error");

    assertEquals(
        "Unable to convert attribute from %s to %s".formatted(expectedInitialDataType, newDataType),
        e.getMessage());
  }

  static Stream<Arguments> invalidConversionTestCases() {
    return Stream.of(
        Arguments.of(Boolean.TRUE, DataTypeMapping.BOOLEAN, DataTypeMapping.DATE),
        Arguments.of(Boolean.TRUE, DataTypeMapping.BOOLEAN, DataTypeMapping.DATE_TIME),
        Arguments.of(LocalDate.of(2024, 1, 24), DataTypeMapping.DATE, DataTypeMapping.BOOLEAN),
        Arguments.of(
            LocalDateTime.of(2024, 1, 24, 2, 30, 0),
            DataTypeMapping.DATE_TIME,
            DataTypeMapping.BOOLEAN));
  }

  @Test
  void deleteAttribute() {
    // Arrange
    createRecordWithAttributes(new String[] {"attr1", "attr2"});

    // Act
    recordOrchestratorService.deleteAttribute(COLLECTION_UUID, VERSION, TEST_TYPE, "attr2");

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
            () ->
                recordOrchestratorService.deleteAttribute(
                    COLLECTION_UUID, VERSION, TEST_TYPE, "id"),
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
                    COLLECTION_UUID, VERSION, TEST_TYPE, "doesnotexist"),
            "deleteAttribute should have thrown an error");
    assertEquals(
        "Attribute does not exist or you do not have permission to see it", e.getMessage());
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
            COLLECTION_UUID,
            VERSION,
            TEST_TYPE,
            RECORD_ID,
            Optional.of(PRIMARY_KEY),
            recordRequest);

    Set<String> expectedAttributes = Stream.of(attributeNames).collect(Collectors.toSet());
    expectedAttributes.add(PRIMARY_KEY);

    assertAttributes(expectedAttributes);
  }

  private void assertAttributes(Set<String> expectedAttributeNames) {
    RecordTypeSchema schema =
        recordOrchestratorService.describeRecordType(COLLECTION_UUID, VERSION, TEST_TYPE);
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
        recordOrchestratorService.describeRecordType(COLLECTION_UUID, VERSION, TEST_TYPE);

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
        recordOrchestratorService.describeAllRecordTypes(COLLECTION_UUID, VERSION);

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
            COLLECTION_UUID, VERSION, newRecordType, newRecordId, Optional.empty(), recordRequest);

    assertEquals(newRecordId, response.getBody().recordId());
  }

  private void testGetRecord(String newRecordId, String testKey, String testVal) {
    RecordResponse recordResponse =
        recordOrchestratorService.getSingleRecord(COLLECTION_UUID, VERSION, TEST_TYPE, newRecordId);
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
        recordOrchestratorService.describeRecordType(COLLECTION_UUID, VERSION, TEST_TYPE);
    AttributeSchema attributeSchema = recordTypeSchema.getAttributeSchema(attributeName);
    assertEquals(dataType.name(), attributeSchema.datatype(), message);
  }

  private void assertAttributeValue(
      String recordId, String attributeName, Object expectedValue, String message) {
    RecordResponse record =
        recordOrchestratorService.getSingleRecord(COLLECTION_UUID, VERSION, TEST_TYPE, recordId);
    Object attributeValue = record.recordAttributes().getAttributeValue(attributeName);

    if (expectedValue instanceof Object[]) {
      assertArrayEquals((Object[]) expectedValue, (Object[]) attributeValue, message);
    } else {
      assertEquals(expectedValue, attributeValue, message);
    }
  }
}
