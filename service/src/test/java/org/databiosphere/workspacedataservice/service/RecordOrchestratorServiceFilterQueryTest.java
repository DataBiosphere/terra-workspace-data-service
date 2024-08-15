package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.search.InvalidQueryException;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchFilter;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for the filter-by-column feature of RecordOrchestratorService.queryForRecords()
 *
 * <p>See also the test data at src/test/resources/searchfilter/testdata.tsv
 */
@ActiveProfiles(profiles = {"mock-sam"})
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordOrchestratorServiceFilterQueryTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  @Autowired private RecordDao recordDao;
  @Autowired private TwdsProperties twdsProperties;

  @Value("classpath:searchfilter/testdata.tsv")
  Resource testDataTsv;

  private UUID testCollectionId;
  private static final RecordType TEST_TYPE = RecordType.valueOf("test");

  private static final String PRIMARY_KEY = "test_id";

  // delete all collections, across all workspaces
  private void cleanupAll() {
    List<CollectionServerModel> colls = collectionService.list(twdsProperties.workspaceId());
    colls.forEach(
        coll ->
            collectionService.delete(twdsProperties.workspaceId(), CollectionId.of(coll.getId())));
  }

  @BeforeEach
  void beforeEach() {
    // delete all existing collections
    cleanupAll();
    // create our collection
    CollectionRequestServerModel coll =
        new CollectionRequestServerModel(
            "unit-test", "RecordOrchestratorServiceFilterQueryTest unit test collection");

    CollectionServerModel actual = collectionService.save(twdsProperties.workspaceId(), coll);

    // save the created collection's id for use in tests
    testCollectionId = actual.getId();
  }

  @AfterAll
  void afterAll() {
    // delete all existing collections
    cleanupAll();
  }

  // ===== STRING column
  private static Stream<Arguments> stringArguments() {
    return Stream.of(
        Arguments.of("\"hello world\"", List.of("1", "2")),
        Arguments.of("\"HELLO WORLD\"", List.of("1", "2")),
        Arguments.of("goodbye", List.of("3")),
        Arguments.of("GOODBYE", List.of("3")),
        Arguments.of("thisValueNotInDataset", List.of()));
  }

  @ParameterizedTest(name = "string filter for value <{0}>")
  @MethodSource("stringArguments")
  void stringColumn(String criteria, List<String> expectedIds) {
    performTest("str", DataTypeMapping.STRING, criteria, expectedIds);
  }

  // ===== ARRAY_OF_STRING column
  private static Stream<Arguments> arrayOfStringArguments() {
    return Stream.of(
        Arguments.of("world", List.of("1", "2")),
        Arguments.of("WORLD", List.of("1", "2")),
        Arguments.of("You", List.of("1")),
        Arguments.of("Three", List.of("3")),
        Arguments.of("thisValueNotInDataset", List.of()));
  }

  @ParameterizedTest(name = "array_of_string filter for value <{0}>")
  @MethodSource("arrayOfStringArguments")
  void arrayOfStringColumn(String criteria, List<String> expectedIds) {
    performTest("arrstr", DataTypeMapping.ARRAY_OF_STRING, criteria, expectedIds);
  }

  // ===== FILE column
  private static Stream<Arguments> fileArguments() {
    return Stream.of(
        Arguments.of(
            "\"drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa\"",
            List.of("1", "2")),
        Arguments.of(
            "\"https://teststorageaccount.blob.core.windows.net/testcontainer/file\"",
            List.of("3")));
  }

  @ParameterizedTest(name = "file filter for value <{0}>")
  @MethodSource("fileArguments")
  void fileColumn(String criteria, List<String> expectedIds) {
    performTest("file", DataTypeMapping.FILE, criteria, expectedIds);
  }

  // ===== ARRAY_OF_FILE column
  private static Stream<Arguments> arrayOfFileArguments() {
    return Stream.of(
        Arguments.of(
            "\"drs://example.org/dg.4503/cc32d93d-a73c-4d2c-a061-26c0410e74fa\"",
            List.of("1", "2")),
        Arguments.of(
            "\"https://teststorageaccount.blob.core.windows.net/testcontainer/file\"",
            List.of("2", "3")));
  }

  @ParameterizedTest(name = "array_of_file filter for value <{0}>")
  @MethodSource("arrayOfFileArguments")
  void arrayOfFileColumn(String criteria, List<String> expectedIds) {
    performTest("arrfile", DataTypeMapping.ARRAY_OF_FILE, criteria, expectedIds);
  }

  // ===== NUMBER column
  private static Stream<Arguments> numberArguments() {
    return Stream.of(
        Arguments.of("42", List.of("1", "2")),
        Arguments.of("\\-1.23", List.of("3")),
        Arguments.of("1.23", List.of()),
        Arguments.of("999999", List.of()));
  }

  @ParameterizedTest(name = "number filter for value <{0}>")
  @MethodSource("numberArguments")
  void numberColumn(String criteria, List<String> expectedIds) {
    performTest("num", DataTypeMapping.NUMBER, criteria, expectedIds);
  }

  // ===== ARRAY_OF_NUMBER column
  private static Stream<Arguments> arrayOfNumberArguments() {
    return Stream.of(
        Arguments.of("31", List.of("1", "2")),
        Arguments.of("59", List.of("1")),
        Arguments.of("3", List.of("3")),
        Arguments.of("9999999", List.of()));
  }

  @ParameterizedTest(name = "array_of_number filter for value <{0}>")
  @MethodSource("arrayOfNumberArguments")
  void arrayOfNumberColumn(String criteria, List<String> expectedIds) {
    performTest("arrnum", DataTypeMapping.ARRAY_OF_NUMBER, criteria, expectedIds);
  }

  // ===== BOOLEAN column
  private static Stream<Arguments> booleanArguments() {
    return Stream.of(
        Arguments.of("true", List.of("1", "2")),
        Arguments.of("TRUE", List.of("1", "2")),
        Arguments.of("false", List.of("3")),
        Arguments.of("FALSE", List.of("3")));
  }

  @ParameterizedTest(name = "boolean filter for value <{0}>")
  @MethodSource("booleanArguments")
  void booleanColumn(String criteria, List<String> expectedIds) {
    performTest("bool", DataTypeMapping.BOOLEAN, criteria, expectedIds);
  }

  // ===== ARRAY_OF_BOOLEAN column
  private static Stream<Arguments> arrayOfBooleanArguments() {
    return Stream.of(
        Arguments.of("true", List.of("1", "2")),
        Arguments.of("TRUE", List.of("1", "2")),
        Arguments.of("false", List.of("2", "3")),
        Arguments.of("FALSE", List.of("2", "3")));
  }

  @ParameterizedTest(name = "array_of_boolean filter for value <{0}>")
  @MethodSource("arrayOfBooleanArguments")
  void arrayOfBooleanColumn(String criteria, List<String> expectedIds) {
    performTest("arrbool", DataTypeMapping.ARRAY_OF_BOOLEAN, criteria, expectedIds);
  }

  // ===== DATE column
  private static Stream<Arguments> dateArguments() {
    return Stream.of(
        Arguments.of("1979-06-25", List.of("1", "2")), Arguments.of("1981-02-12", List.of("3")));
  }

  @ParameterizedTest(name = "date filter for value <{0}>")
  @MethodSource("dateArguments")
  void dateColumn(String criteria, List<String> expectedIds) {
    performTest("date", DataTypeMapping.DATE, criteria, expectedIds);
  }

  // ===== ARRAY_OF_DATE column
  private static Stream<Arguments> arrayOfDateArguments() {
    return Stream.of(
        Arguments.of("1971-11-12", List.of("1", "2")),
        Arguments.of("1977-01-21", List.of("2", "3")));
  }

  @ParameterizedTest(name = "array_of_date filter for value <{0}>")
  @MethodSource("arrayOfDateArguments")
  void arrayOfDateColumn(String criteria, List<String> expectedIds) {
    performTest("arrdate", DataTypeMapping.ARRAY_OF_DATE, criteria, expectedIds);
  }

  // ===== DATETIME column
  private static Stream<Arguments> datetimeArguments() {
    return Stream.of(
        Arguments.of("\"2024-08-13T19:00:00\"", List.of("1")),
        Arguments.of("\"2024-08-12T18:00:00\"", List.of("2")),
        Arguments.of("\"2024-08-11T17:00:00\"", List.of("3")));
  }

  @ParameterizedTest(name = "datetime filter for value <{0}>")
  @MethodSource("datetimeArguments")
  void datetimeColumn(String criteria, List<String> expectedIds) {
    performTest("datetime", DataTypeMapping.DATE_TIME, criteria, expectedIds);
  }

  // ===== ARRAY_OF_DATETIME column
  private static Stream<Arguments> arrayOfDatetimeArguments() {
    return Stream.of(
        Arguments.of("\"2024-08-10T16:00:00\"", List.of("1")),
        Arguments.of("\"2024-08-09T15:00:00\"", List.of("2")),
        Arguments.of("\"2024-08-08T14:00:00\"", List.of("3")));
  }

  @ParameterizedTest(name = "array_of_datetime filter for value <{0}>")
  @MethodSource("arrayOfDatetimeArguments")
  void arrayOfDatetimeColumn(String criteria, List<String> expectedIds) {
    performTest("arrdatetime", DataTypeMapping.ARRAY_OF_DATE_TIME, criteria, expectedIds);
  }

  // the test implementation for all tests above
  private void performTest(
      String columnName, DataTypeMapping datatype, String criteria, List<String> expectedIds) {
    // init the record type. This allows us to query the table prior to inserting any records.
    Map<String, DataTypeMapping> schema = Map.of(columnName, datatype);
    recordDao.createRecordType(
        testCollectionId, schema, TEST_TYPE, RelationCollection.empty(), PRIMARY_KEY);

    // build the by-column filter
    SearchFilter searchFilter =
        new SearchFilter(Optional.empty(), Optional.of(columnName + ":" + criteria));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    // perform the filter, should return zero
    filterAndExpect(List.of(), searchRequest);

    // load the test data
    loadTestData();

    // perform the filter again, should return expected results
    filterAndExpect(expectedIds, searchRequest);
  }

  @Test
  void unknownColumn() {
    // init the record type. This allows us to query the table prior to inserting any records.
    Map<String, DataTypeMapping> schema = Map.of("test-column", DataTypeMapping.STRING);
    recordDao.createRecordType(
        testCollectionId, schema, TEST_TYPE, RelationCollection.empty(), PRIMARY_KEY);

    // build the by-column filter, using a column name that doesn't exist in the record type
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of("unknown:criteria"));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    InvalidQueryException validationException =
        assertThrows(
            InvalidQueryException.class,
            () ->
                recordOrchestratorService.queryForRecords(
                    testCollectionId, TEST_TYPE, VERSION, searchRequest));
    assertEquals(
        "Column specified in query does not exist in this record type",
        validationException.getMessage());
  }

  // if the user specifies filter.query but provides ""
  @Test
  void emptyQuery() {
    // build the search request, specifying "" for query
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(""));
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    // load the test data
    loadTestData();

    // perform the filter, should return three
    filterAndExpect(List.of("1", "2", "3"), searchRequest);
  }

  // what if the user specifies the primary key column for the filter?
  @Test
  void filterOnPrimaryKey() {
    // load the test data
    loadTestData();

    // for each inserted record, perform a search for its id and expect to find just that record
    List.of("1", "2", "3")
        .forEach(
            id -> {
              SearchFilter searchFilter =
                  new SearchFilter(Optional.empty(), Optional.of(PRIMARY_KEY + ":" + id));
              SearchRequest searchRequest = new SearchRequest();
              searchRequest.setFilter(Optional.of(searchFilter));
              filterAndExpect(List.of(id), searchRequest);
            });
  }

  // can users search for null as a column value?
  @Disabled("not implemented yet")
  @Test
  void filterForNull() {
    // AJ-1238
    fail("not implemented yet");
  }

  // can users search multiple columns at once?
  @Disabled("we don't support multiple columns yet")
  @Test
  void filterMultipleColumns() {
    // AJ-1238
    fail("not implemented yet");
  }

  // the "totalRecords" value in the response is the *unfiltered* count. Should it be the filtered
  // count? Should we add a new value in the response for the filtered count? If a new value,
  // should we wait for a v1 API to add that?
  @Disabled("need decisions on the response payload")
  @Test
  void totalRecordsIsCorrect() {
    // TODO AJ-1238
    fail("need decisions on the response payload");
  }

  private void filterAndExpect(List<String> expectedIds, SearchRequest searchRequest) {
    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            testCollectionId, TEST_TYPE, VERSION, searchRequest);
    assertEquals(expectedIds.size(), resp.records().size(), "incorrect result count");
    // extract record ids from the response
    List<String> actualIds = resp.records().stream().map(RecordResponse::recordId).toList();
    assertThat(expectedIds).hasSameElementsAs(actualIds);
  }

  private void loadTestData() {
    try {
      MockMultipartFile testTsv =
          new MockMultipartFile("testdata.tsv", testDataTsv.getInputStream());
      recordOrchestratorService.tsvUpload(
          testCollectionId, "v0.2", TEST_TYPE, Optional.empty(), testTsv);
    } catch (Exception e) {
      fail(e);
    }
  }
}
