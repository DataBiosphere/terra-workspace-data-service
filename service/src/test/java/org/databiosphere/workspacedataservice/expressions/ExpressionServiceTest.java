package org.databiosphere.workspacedataservice.expressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
public class ExpressionServiceTest extends ControlPlaneTestBase {
  @Autowired private ExpressionService expressionService;
  @Autowired private RecordDao recordDao;
  @Autowired private CollectionService collectionService;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private RecordService recordService;
  @Autowired private WorkspaceRepository workspaceRepository;

  UUID collectionUuid;

  @BeforeEach
  void setUp() {
    var workspaceId = WorkspaceId.of(UUID.randomUUID());

    // create the workspace record
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));
    // create the collection
    collectionUuid = collectionService.save(workspaceId, "name", "desc").getId();
  }

  @AfterEach
  void tearDown() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  @Test
  void testExtractRecordAttributeLookups() {
    var result =
        expressionService.extractRecordAttributeLookups(
            """
                { "foo" : this.foo.attribute, "bar" : this.foo.attribute2, "baz" : this.attribute }""");

    assertThat(result)
        .hasSameElementsAs(
            List.of(
                new AttributeLookup(List.of("foo"), "attribute", "this.foo.attribute"),
                new AttributeLookup(List.of("foo"), "attribute2", "this.foo.attribute2"),
                new AttributeLookup(List.of(), "attribute", "this.attribute")));
  }

  @Test
  void testGetArrayRelations() {
    var grandChildType = RecordType.valueOf("grandChildType");
    var childType = RecordType.valueOf("childType");
    var parentType = RecordType.valueOf("parentType");

    var grandRelation = new Relation("grand", grandChildType);
    var sib1Relation = new Relation("sib1", childType);
    var sib2Relation = new Relation("sib2", childType);

    var pkAttr = "pk";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(pkAttr, DataTypeMapping.STRING),
        grandChildType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(grandRelation.relationColName(), DataTypeMapping.RELATION),
        childType,
        new RelationCollection(Set.of(), Set.of(grandRelation)),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(
            sib1Relation.relationColName(),
            DataTypeMapping.RELATION,
            sib2Relation.relationColName(),
            DataTypeMapping.RELATION),
        parentType,
        new RelationCollection(Set.of(sib1Relation, sib2Relation), Set.of()),
        pkAttr);

    var results =
        expressionService.getArrayRelations(collectionUuid, parentType, "this.sib2.grand");

    assertThat(results).hasSameElementsAs(List.of(sib2Relation, grandRelation));
  }

  @Test
  void testGetArrayRelationsMissingRelation() {
    var childType = RecordType.valueOf("childType");
    var parentType = RecordType.valueOf("parentType");

    var sib1Relation = new Relation("sib1", childType);
    var sib2Relation = new Relation("sib2", childType);

    var pkAttr = "pk";

    recordDao.createRecordType(
        collectionUuid, Map.of(), childType, RelationCollection.empty(), pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(
            sib1Relation.relationColName(),
            DataTypeMapping.RELATION,
            sib2Relation.relationColName(),
            DataTypeMapping.RELATION),
        parentType,
        new RelationCollection(Set.of(sib1Relation, sib2Relation), Set.of()),
        pkAttr);

    assertThatThrownBy(
            () ->
                expressionService.getArrayRelations(
                    collectionUuid, parentType, "this.sib2.missing"))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            e -> {
              assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
              assertThat(e.getReason()).contains("missing");
            });
  }

  /**
   * Tests that given the relations parentType -> childType -> grandChildType,
   * determineExpressionQueries will return the correct ExpressionQueryInfo objects for the
   * attribute lookups from expressions this.pk, this.parentAttr, this.sib1.grand.grandChildAttr,
   * this.sib1.grand.pk, this.sib2.grand.grandChildAttr, this.sib1.childAttr.
   */
  @Test
  void testDetermineExpressionQueries() {
    var grandChildType = RecordType.valueOf("grandChildType");
    var childType = RecordType.valueOf("childType");
    var parentType = RecordType.valueOf("parentType");

    var grandRelation = new Relation("grand", grandChildType);
    var sib1Relation = new Relation("sib1", childType);
    var sib2Relation = new Relation("sib2", childType);

    var pkAttr = "pk";
    var parentAttr = "parentAttr";
    var childAttr = "childAttr";
    var grandChildAttr = "grandChildAttr";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(grandChildAttr, DataTypeMapping.STRING, pkAttr, DataTypeMapping.STRING),
        grandChildType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(
            childAttr,
            DataTypeMapping.STRING,
            pkAttr,
            DataTypeMapping.STRING,
            grandRelation.relationColName(),
            DataTypeMapping.RELATION),
        childType,
        new RelationCollection(Set.of(grandRelation), Set.of()),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(
            parentAttr,
            DataTypeMapping.STRING,
            pkAttr,
            DataTypeMapping.STRING,
            sib1Relation.relationColName(),
            DataTypeMapping.RELATION,
            sib2Relation.relationColName(),
            DataTypeMapping.RELATION),
        parentType,
        new RelationCollection(Set.of(sib1Relation, sib2Relation), Set.of()),
        pkAttr);

    var parentAttrLookup = new AttributeLookup(List.of(), parentAttr, "this.parentAttr");
    var parentPkLookup = new AttributeLookup(List.of(), pkAttr, "this.pk");
    var sib1GrandChildAttrLookup =
        new AttributeLookup(
            List.of(sib1Relation.relationColName(), grandRelation.relationColName()),
            grandChildAttr,
            "this.sib1.grand.grandChildAttr");
    var sib1GrandChildPkLookup =
        new AttributeLookup(
            List.of(sib1Relation.relationColName(), grandRelation.relationColName()),
            pkAttr,
            "this.sib1.grand.pk");
    var sib2GrandChildAttrLookup =
        new AttributeLookup(
            List.of(sib2Relation.relationColName(), grandRelation.relationColName()),
            grandChildAttr,
            "this.sib2.grand.grandChildAttr");
    var sib1ChildAttrLookup =
        new AttributeLookup(
            List.of(sib1Relation.relationColName()), childAttr, "this.sib1.childAttr");
    var results =
        expressionService
            .determineExpressionQueries(
                collectionUuid,
                parentType,
                Set.of(
                    parentAttrLookup,
                    parentPkLookup,
                    sib1GrandChildAttrLookup,
                    sib1GrandChildPkLookup,
                    sib2GrandChildAttrLookup,
                    sib1ChildAttrLookup),
                0)
            .toList();

    // there are 6 lookups which are reduced to 4 queries
    // 1 on the root (no relations) with 2 attributes, 1 on sib1 with 1 attribute,
    // 1 on sib1 -> grand with 2 attributes, 1 on sib2 -> grand with 1 attribute
    // note that there is no sib2 query because there are no lookups with sib2 attributes
    assertThat(results)
        .hasSameElementsAs(
            List.of(
                new ExpressionQueryInfo(List.of(), Set.of(parentAttrLookup, parentPkLookup), false),
                new ExpressionQueryInfo(List.of(sib1Relation), Set.of(sib1ChildAttrLookup), false),
                new ExpressionQueryInfo(
                    List.of(sib1Relation, grandRelation),
                    Set.of(sib1GrandChildAttrLookup, sib1GrandChildPkLookup),
                    false),
                new ExpressionQueryInfo(
                    List.of(sib2Relation, grandRelation),
                    Set.of(sib2GrandChildAttrLookup),
                    false)));
  }

  @Test
  void testDetermineExpressionQueriesMissingRelation() {
    var recordType = RecordType.valueOf("recordType");

    var pkAttr = "pk";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(pkAttr, DataTypeMapping.STRING),
        recordType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);

    var missingRelation = "missingRelation";
    assertThatThrownBy(
            () ->
                expressionService.determineExpressionQueries(
                    collectionUuid,
                    recordType,
                    Set.of(
                        new AttributeLookup(
                            List.of(missingRelation),
                            "missingAttr",
                            "this.missingRelation.missingAttr")),
                    0))
        .isInstanceOfSatisfying(
            ResponseStatusException.class,
            e -> {
              assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
              assertThat(e.getReason()).contains(missingRelation);
            });
  }

  Stream<Arguments> testGetExpressionResultLookupMapParams() {
    var localDate = new Date(Instant.now().toEpochMilli()).toLocalDate();
    // adjust the Instant to have a millisecond value to ensure the timestamp is not truncated
    var localDateTime =
        new Timestamp(Instant.now().with(ChronoField.MILLI_OF_SECOND, 111).toEpochMilli())
            .toLocalDateTime();
    var jsonValue = objectMapper.valueToTree(Map.of("key", "value"));
    LocalDate[] localDateArray = new LocalDate[] {localDate};
    LocalDateTime[] localDateTimeArray = new LocalDateTime[] {localDateTime};
    JsonNode[] jsonValueArray = new JsonNode[] {jsonValue};
    List<LocalDate[]> localDateList = new ArrayList<>();
    localDateList.add(localDateArray);
    List<LocalDateTime[]> localDateTimeList = new ArrayList<>();
    localDateTimeList.add(localDateTimeArray);
    List<JsonNode[]> jsonValueList = new ArrayList<>();
    jsonValueList.add(jsonValueArray);

    return Stream.of(
        // scalar values
        Arguments.of(List.of(1), "1"),
        Arguments.of(List.of("a"), "\"a\""),
        Arguments.of(List.of(true), "true"),
        Arguments.of(List.of(1.1), "1.1"),
        Arguments.of(List.of(localDate), "\"" + localDate + "\""),
        Arguments.of(List.of(localDateTime), "\"" + localDateTime + "\""),
        Arguments.of(List.of(jsonValue), "{\"key\":\"value\"}"),

        // arrays of values
        Arguments.of(List.of(1, 2, 3, 4, 5), "[1,2,3,4,5]"),
        Arguments.of(List.of("a", "b", "c", "d", "e"), "[\"a\",\"b\",\"c\",\"d\",\"e\"]"),
        Arguments.of(List.of(true, false, true, false, true), "[true,false,true,false,true]"),
        Arguments.of(List.of(1.1, 2.2, 3.3, 4.4, 5.5), "[1.1,2.2,3.3,4.4,5.5]"),
        Arguments.of(
            List.of(localDate, localDate), "[\"" + localDate + "\",\"" + localDate + "\"]"),
        Arguments.of(
            List.of(localDateTime, localDateTime),
            "[\"" + localDateTime + "\",\"" + localDateTime + "\"]"),
        Arguments.of(List.of(jsonValue, jsonValue), "[{\"key\":\"value\"},{\"key\":\"value\"}]"),

        // values that are arrays
        Arguments.of(List.of(new int[] {1, 2}), "[1,2]"),
        Arguments.of(List.of(new String[] {"a", "b"}), "[\"a\",\"b\"]"),
        Arguments.of(List.of(new boolean[] {true, false}), "[true,false]"),
        Arguments.of(List.of(new double[] {1.1, 2.2}), "[1.1,2.2]"),
        Arguments.of(localDateList, "[\"" + localDate + "\"]"),
        Arguments.of(localDateTimeList, "[\"" + localDateTime + "\"]"),
        Arguments.of(jsonValueList, "[{\"key\":\"value\"}]"),

        // arrays of values that are arrays
        Arguments.of(List.of(new int[] {1, 2}, new int[] {3, 4}), "[[1,2],[3,4]]"),
        Arguments.of(
            List.of(new String[] {"a", "b"}, new String[] {"c", "d"}),
            "[[\"a\",\"b\"],[\"c\",\"d\"]]"),
        Arguments.of(
            List.of(new boolean[] {true, false}, new boolean[] {false, true}),
            "[[true,false],[false,true]]"),
        Arguments.of(
            List.of(new double[] {1.1, 2.2}, new double[] {3.3, 4.4}), "[[1.1,2.2],[3.3,4.4]]"),
        Arguments.of(
            List.of(localDateArray, localDateArray),
            "[[\"" + localDate + "\"],[\"" + localDate + "\"]]"),
        Arguments.of(
            List.of(localDateTimeArray, localDateTimeArray),
            "[[\"" + localDateTime + "\"],[\"" + localDateTime + "\"]]"),
        Arguments.of(
            List.of(jsonValueArray, jsonValueArray),
            "[[{\"key\":\"value\"}],[{\"key\":\"value\"}]]"));
  }

  @ParameterizedTest
  @MethodSource("testGetExpressionResultLookupMapParams")
  void testGetExpressionResultLookupMap(List<Object> recordValues, String expectedJson) {
    var recordType = RecordType.valueOf("recordType");
    var records =
        recordValues.stream()
            .map(
                value ->
                    new Record(
                        UUID.randomUUID().toString(),
                        recordType,
                        new RecordRequest(new RecordAttributes(Map.of("value", value)))))
            .toList();

    var result =
        expressionService.getExpressionResultLookupMap(
            Map.of(
                new ExpressionQueryInfo(
                    List.of(),
                    Set.of(
                        new AttributeLookup(List.of(), "value", "this.value"),
                        new AttributeLookup(List.of(), "recordtype_id", "this.recordtype_id")),
                    recordValues.size() > 1),
                records,
                new ExpressionQueryInfo(
                    List.of(),
                    Set.of(new AttributeLookup(List.of(), "value", "this.that.value")),
                    recordValues.size() > 1),
                records),
            recordType);

    assertThat(result).hasSize(3);
    assertThat(result.get("this.value").toString()).isEqualTo(expectedJson);
    assertThat(result.get("this.that.value").toString()).isEqualTo(expectedJson);
    assertThat(result).hasEntrySatisfying("this.recordtype_id", v -> assertThat(v).isNotNull());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetExpressionResultLookupMapMissingValue(boolean isArray) {
    var recordType = RecordType.valueOf("recordType");
    var record =
        new Record(
            UUID.randomUUID().toString(), recordType, new RecordRequest(RecordAttributes.empty()));

    var result =
        expressionService.getExpressionResultLookupMap(
            Map.of(
                new ExpressionQueryInfo(
                    List.of(),
                    Set.of(new AttributeLookup(List.of(), "missing", "this.missing")),
                    isArray),
                List.of(record)),
            recordType);

    assertThat(result).hasSize(1);
    assertThat(result.get("this.missing").toString()).isEqualTo(isArray ? "[]" : "null");
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "this.value|1",
        "{ \"value\" : this.value }|{\"value\":1}",
        "{ \"foo\" : this.value, \"bar\" : this.value }|{\"foo\":1,\"bar\":1}"
      },
      delimiter = '|')
  void testSubstituteResultsInExpressions(String expression, String expected) {
    var recordType = RecordType.valueOf("recordType");
    var records =
        List.of(
            new Record(
                UUID.randomUUID().toString(),
                recordType,
                new RecordRequest(new RecordAttributes(Map.of("value", 1)))));

    var expressionName = "test";
    var result =
        expressionService.substituteResultsInExpressions(
            Map.of(expressionName, expression),
            Map.of(
                new ExpressionQueryInfo(
                    List.of(),
                    Set.of(new AttributeLookup(List.of(), "value", "this.value")),
                    false),
                records),
            recordType);

    assertThat(result).hasSize(1);
    assertThat(result.get(expressionName).toString()).isEqualTo(expected);
  }

  @Test
  @Transactional
  void testEvaluateExpressions() {
    var recordType = RecordType.valueOf("recordType");

    var pkAttr = "pk";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(pkAttr, DataTypeMapping.STRING),
        recordType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);

    var recordId = UUID.randomUUID().toString();
    var record = new Record(recordId, recordType, new RecordRequest(RecordAttributes.empty()));
    recordDao.batchUpsert(collectionUuid, recordType, List.of(record), Map.of());

    var expressionName = "test";
    var expression = "this.recordType_id";
    var result =
        expressionService.evaluateExpressions(
            collectionUuid, VERSION, recordType, recordId, Map.of(expressionName, expression));

    assertThat(result).hasSize(1);
    assertThat(result.get(expressionName).toString()).isEqualTo("\"" + recordId + "\"");
  }

  @Test
  @Transactional
  void testEvaluateExpressionsWithRelationArray() {
    var recordType = RecordType.valueOf("recordType");
    var nestedRecordType = RecordType.valueOf("nestedRecordType");

    var pkAttr = "pk";

    recordDao.createRecordType(
        collectionUuid,
        Map.of(pkAttr, DataTypeMapping.STRING),
        recordType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);
    recordDao.createRecordType(
        collectionUuid,
        Map.of(pkAttr, DataTypeMapping.STRING),
        nestedRecordType,
        new RelationCollection(Set.of(), Set.of()),
        pkAttr);

    var nestedRecords =
        Stream.of(1, 2, 3)
            .map(
                i ->
                    new Record(
                        UUID.randomUUID().toString(),
                        nestedRecordType,
                        new RecordRequest(RecordAttributes.empty())))
            .toList();
    recordDao.batchUpsert(collectionUuid, nestedRecordType, nestedRecords, Map.of());

    var recordId = UUID.randomUUID().toString();
    var record = new Record(recordId, recordType, new RecordRequest(RecordAttributes.empty()));
    recordDao.batchUpsert(collectionUuid, recordType, List.of(record), Map.of());
    recordService.updateSingleRecord(
        collectionUuid,
        recordType,
        record.getId(),
        new RecordRequest(
            new RecordAttributes(
                Map.of(
                    "arrayAttr",
                    nestedRecords.stream()
                        .map(r -> RelationUtils.createRelationString(nestedRecordType, r.getId()))
                        .toList()))));

    var expressionName = "test";
    var expression = "this.%s_id".formatted(nestedRecordType.getName());
    var result =
        expressionService.evaluateExpressionsWithRelationArray(
            collectionUuid,
            VERSION,
            recordType,
            recordId,
            "this.arrayAttr",
            Map.of(expressionName, expression),
            10,
            0);

    assertThat(result).hasSize(nestedRecords.size());
    result.forEach(
        (nestedRecordId, nestedResult) -> {
          assertThat(nestedResult.get(expressionName).toString())
              .isEqualTo("\"" + nestedRecordId + "\"");
        });
  }
}
