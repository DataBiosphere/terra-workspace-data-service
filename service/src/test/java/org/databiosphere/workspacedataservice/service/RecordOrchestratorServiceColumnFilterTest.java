package org.databiosphere.workspacedataservice.service;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.databiosphere.workspacedata.model.FilterColumn;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

/** Tests for the filter-by-column feature of RecordOrchestratorService.queryForRecords() */
@ActiveProfiles(profiles = {"mock-sam"})
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordOrchestratorServiceColumnFilterTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  @Autowired private RecordDao recordDao;
  @Autowired private TwdsProperties twdsProperties;

  private static final UUID COLLECTION_UUID = randomUUID();
  private static final RecordType TEST_TYPE = RecordType.valueOf("test");

  private static final String PRIMARY_KEY = "id";
  private static final String TEST_COLUMN = "test-column";
  private static final String CONTROL_COLUMN = "control-column";

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
    CollectionServerModel coll =
        new CollectionServerModel(
            "unit-test", "RecordOrchestratorServiceColumnFilterTest unit test collection");
    coll.id(COLLECTION_UUID);
    collectionService.save(twdsProperties.workspaceId(), coll);
  }

  @AfterAll
  void afterAll() {
    // delete all existing collections
    cleanupAll();
  }

  @ParameterizedTest(name = "string filter for value <{0}>")
  @ValueSource(strings = {"lowercase", "Sentencecase", "UPPERCASE", "Phrase, with punctuation!"})
  void stringFilter(String criteria) {
    // init the record type. This allows us to query the table prior to inserting any records.
    Map<String, DataTypeMapping> schema =
        Map.of(TEST_COLUMN, DataTypeMapping.STRING, CONTROL_COLUMN, DataTypeMapping.STRING);
    recordDao.createRecordType(
        COLLECTION_UUID, schema, TEST_TYPE, RelationCollection.empty(), PRIMARY_KEY);

    // build the by-column filter
    FilterColumn filterColumn = new FilterColumn().column(TEST_COLUMN).find(criteria);
    List<FilterColumn> filterColumns = List.of(filterColumn);

    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    // what value should we use for the control column?
    Supplier<Object> controlValueSupplier = () -> "don't match on" + criteria + ", please";

    // perform the filter, should return zero
    filterAndExpect(0, List.of(), searchRequest);

    // insert a few filler records, then filter again; should still return zero
    insertFillerRecords(3, criteria, controlValueSupplier);
    filterAndExpect(0, List.of(), searchRequest);

    // insert a findable record and a few more filler records, then filter; should return 1
    List<String> findOne = insertFindableRecords(1, criteria);
    insertFillerRecords(3, criteria, controlValueSupplier);
    filterAndExpect(1, findOne, searchRequest);

    // insert two more findable records, then filter; should return 3
    List<String> findTwo = insertFindableRecords(2, criteria);
    List<String> findOneAndTwo = Stream.concat(findOne.stream(), findTwo.stream()).toList();
    filterAndExpect(3, findOneAndTwo, searchRequest);
  }

  @Test
  void unknownColumn() {
    // init the record type. This allows us to query the table prior to inserting any records.
    Map<String, DataTypeMapping> schema =
        Map.of(TEST_COLUMN, DataTypeMapping.STRING, CONTROL_COLUMN, DataTypeMapping.STRING);
    recordDao.createRecordType(
        COLLECTION_UUID, schema, TEST_TYPE, RelationCollection.empty(), PRIMARY_KEY);

    // build the by-column filter, using a column name that doesn't exist in the record type
    FilterColumn filterColumn = new FilterColumn().column("unknown").find("criteria");
    List<FilterColumn> filterColumns = List.of(filterColumn);

    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    ValidationException validationException =
        assertThrows(
            ValidationException.class,
            () ->
                recordOrchestratorService.queryForRecords(
                    COLLECTION_UUID, TEST_TYPE, VERSION, searchRequest));
    assertEquals(
        "Specified filter column does not exist in this record type",
        validationException.getMessage());
  }

  // if the user specifies filter.filters but provides []
  @Test
  void emptyArrayOfFilters() {
    String criteria = "foo";

    // build the search request, specifying [] for filter columns
    List<FilterColumn> filterColumns = List.of();
    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    // what value should we use for the control column?
    Supplier<Object> controlValueSupplier = () -> "don't match on" + criteria + ", please";

    // create some records
    List<String> recordIds = insertFillerRecords(3, criteria, controlValueSupplier);

    // perform the filter, should return three
    filterAndExpect(3, recordIds, searchRequest);
  }

  // what if the user specifies the primary key column for the filter?
  // see the "why doesn't this include the primary key column?" to-do comment in
  // RecordOrchestratorService.
  // Filtering on the PK column will not work until that to-do is resolved.
  @Disabled("See RecordOrchestratorService")
  @Test
  void filterOnPrimaryKey() {}

  // can users search for null as a column value?
  @Disabled("not implemented yet")
  @Test
  void filterForNull() {}

  // can users search multiple columns at once?
  @Test
  void filterMultipleColumns() {
    // init the record type. This allows us to query the table prior to inserting any records.
    Map<String, DataTypeMapping> schema =
        Map.of(TEST_COLUMN, DataTypeMapping.STRING, CONTROL_COLUMN, DataTypeMapping.STRING);
    recordDao.createRecordType(
        COLLECTION_UUID, schema, TEST_TYPE, RelationCollection.empty(), PRIMARY_KEY);

    String criteria1 = "mack";
    String criteria2 = "bentley";

    // build the by-column filters: separate criteria for test and control columns
    List<FilterColumn> filterColumns =
        List.of(
            new FilterColumn().column(TEST_COLUMN).find(criteria1),
            new FilterColumn().column(CONTROL_COLUMN).find(criteria2));

    SearchFilter searchFilter = new SearchFilter(Optional.empty(), Optional.of(filterColumns));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.setFilter(Optional.of(searchFilter));

    // what value should we use for the control column?
    Supplier<Object> controlValueSupplier = () -> UUID.randomUUID().toString();

    // perform the filter, should return zero
    filterAndExpect(0, List.of(), searchRequest);

    // insert a few filler records, then filter again; should still return zero
    insertFillerRecords(3, criteria1, controlValueSupplier);
    filterAndExpect(0, List.of(), searchRequest);

    // insert a findable record and a few more filler records, then filter; should return 1
    // these findable records use insertFillerRecords() to create records with findable
    // criteria in the control column.
    List<String> findOne = insertFillerRecords(1, criteria2, () -> criteria1);
    insertFillerRecords(3, criteria1, controlValueSupplier);
    filterAndExpect(1, findOne, searchRequest);

    // insert two more findable records, then filter; should return 3
    // these findable records use insertFillerRecords() to create records with findable
    // criteria in the control column.
    List<String> findTwo = insertFillerRecords(2, criteria2, () -> criteria1);
    List<String> findOneAndTwo = Stream.concat(findOne.stream(), findTwo.stream()).toList();
    filterAndExpect(3, findOneAndTwo, searchRequest);
  }

  // the "totalRecords" value in the response is the *unfiltered* count. Should it be the filtered
  // count? Should we add a new value in the response for the filtered count? If a new value,
  // should we wait for a v1 API to add that?
  @Disabled("need decisions on the response payload")
  @Test
  void totalRecordsIsCorrect() {}

  private void filterAndExpect(
      int expectedCount, List<String> expectedIds, SearchRequest searchRequest) {
    RecordQueryResponse resp =
        recordOrchestratorService.queryForRecords(
            COLLECTION_UUID, TEST_TYPE, VERSION, searchRequest);
    assertEquals(expectedCount, resp.records().size(), "incorrect result count");
    // extract record ids from the response
    List<String> actualIds = resp.records().stream().map(RecordResponse::recordId).toList();
    assertThat(expectedIds).hasSameElementsAs(actualIds);
  }

  // create a record with a value which should be found by our filter. This record has the
  // expected value in both the TEST and CONTROL columns; the filter should find it in the
  // TEST column.
  private List<String> insertFindableRecords(int count, Object testVal) {
    RecordAttributes attrs = RecordAttributes.empty(PRIMARY_KEY);
    attrs.putAttribute(TEST_COLUMN, testVal);
    attrs.putAttribute(CONTROL_COLUMN, testVal);
    return IntStream.range(0, count).mapToObj(i -> insertRecord(attrs)).toList();
  }

  // create a record with a value which should NOT be found by our filter. This record has the
  // expected value in the CONTROL column and a dummy value in the TEST column; the filter should
  // hit on neither of these.
  private List<String> insertFillerRecords(
      int count, Object testVal, Supplier<Object> dummySupplier) {
    RecordAttributes attrs = RecordAttributes.empty(PRIMARY_KEY);
    attrs.putAttribute(TEST_COLUMN, dummySupplier.get());
    attrs.putAttribute(CONTROL_COLUMN, testVal);
    return IntStream.range(0, count).mapToObj(i -> insertRecord(attrs)).toList();
  }

  private String insertRecord(RecordAttributes attrs) {
    String newRecordId = UUID.randomUUID().toString();

    RecordRequest recordRequest = new RecordRequest(attrs);

    ResponseEntity<RecordResponse> response =
        recordOrchestratorService.upsertSingleRecord(
            COLLECTION_UUID,
            VERSION,
            TEST_TYPE,
            newRecordId,
            Optional.of(PRIMARY_KEY),
            recordRequest);

    assertEquals(newRecordId, Objects.requireNonNull(response.getBody()).recordId());

    return newRecordId;
  }
}
