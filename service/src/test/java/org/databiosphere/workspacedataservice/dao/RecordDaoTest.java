package org.databiosphere.workspacedataservice.dao;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_NUMBER;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_STRING;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.RelationValue;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordDaoTest extends ControlPlaneTestBase {

  private static final String PRIMARY_KEY = "row_id";
  @Autowired RecordDao recordDao;

  @Autowired CollectionService collectionService;
  @Autowired WorkspaceRepository workspaceRepository;

  UUID collectionUuid;
  WorkspaceId workspaceId;
  RecordType recordType;

  @Autowired TestDao testDao;

  @Autowired NamedParameterJdbcTemplate namedTemplate;

  @BeforeEach
  void setUp() {
    recordType = RecordType.valueOf("testRecordType");
    workspaceId = WorkspaceId.of(UUID.randomUUID());

    // create the workspace record
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));
    // create the collection
    collectionUuid = collectionService.save(workspaceId, "name", "desc").getId();
    // create the record type
    recordDao.createRecordType(
        collectionUuid, emptyMap(), recordType, RelationCollection.empty(), PRIMARY_KEY);
  }

  @AfterEach
  void tearDown() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  @Test
  @Transactional
  void testGetSingleRecord() {
    // add record
    String recordId = "testRecord";
    Record testRecord = new Record(recordId, recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, recordType, Collections.singletonList(testRecord), emptyMap());

    // make sure record is fetched
    Record search = recordDao.getSingleRecord(collectionUuid, recordType, recordId).get();
    assertEquals(testRecord, search);

    // nonexistent record should be null
    Optional<Record> none = recordDao.getSingleRecord(collectionUuid, recordType, "noRecord");
    assertEquals(none, Optional.empty());
  }

  @Test
  void testGetRecordAttributeCaseSensitivity() {
    // Arrange
    RecordType recordType = RecordType.valueOf("aRecord");
    Record upsertedRecord =
        new Record(
            "1",
            recordType,
            RecordAttributes.empty()
                .putAttribute("FOO", "FOO")
                .putAttribute("foo", "foo")
                .putAttribute("Foo", "Foo"));

    recordDao.createRecordType(
        collectionUuid,
        Map.of("FOO", STRING, "foo", STRING, "Foo", STRING),
        recordType,
        RelationCollection.empty(),
        "id");
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        Collections.singletonList(upsertedRecord),
        Map.of("FOO", STRING, "foo", STRING, "Foo", STRING));

    // Act
    List<Record> queryRes =
        recordDao.queryForRecords(recordType, 10, 0, "ASC", null, Optional.empty(), collectionUuid);

    // Assert
    Record returnedRecord = queryRes.get(0);
    assertEquals("FOO", returnedRecord.getAttributes().getAttributeValue("FOO"));
    assertEquals("foo", returnedRecord.getAttributes().getAttributeValue("foo"));
    assertEquals("Foo", returnedRecord.getAttributes().getAttributeValue("Foo"));
  }

  @Test
  void deleteAndQueryFunkyPrimaryKeyValues() {
    RecordType funkyPk = RecordType.valueOf("funkyPk");
    String sample_id = "Sample ID";
    String recordId = "1199";
    Record testRecord = new Record(recordId, funkyPk, RecordAttributes.empty());
    recordDao.createRecordType(
        collectionUuid, Map.of("attr1", STRING), funkyPk, RelationCollection.empty(), sample_id);
    recordDao.batchUpsert(
        collectionUuid, funkyPk, Collections.singletonList(testRecord), emptyMap(), sample_id);
    List<Record> queryRes =
        recordDao.queryForRecords(funkyPk, 10, 0, "ASC", null, Optional.empty(), collectionUuid);
    assertEquals(1, queryRes.size());
    assertTrue(recordDao.recordExists(collectionUuid, funkyPk, recordId));
    assertTrue(recordDao.getSingleRecord(collectionUuid, funkyPk, recordId).isPresent());
    assertTrue(recordDao.deleteSingleRecord(collectionUuid, funkyPk, recordId));
  }

  @Test
  void batchDeleteAndTestRelationsFunkyPrimaryKeyValues() {
    RecordType referencedRt = RecordType.valueOf("referenced");
    String sample_id = "Sample ID";
    String recordId = "1199";
    Record testRecord = new Record(recordId, referencedRt, RecordAttributes.empty());
    Map<String, DataTypeMapping> schema = Map.of("attr1", RELATION);
    recordDao.createRecordType(
        collectionUuid, schema, referencedRt, RelationCollection.empty(), sample_id);
    recordDao.batchUpsert(
        collectionUuid, referencedRt, Collections.singletonList(testRecord), emptyMap(), sample_id);
    RecordType referencer = RecordType.valueOf("referencer");
    recordDao.createRecordType(
        collectionUuid,
        schema,
        referencer,
        new RelationCollection(
            Collections.singleton(new Relation("attr1", referencedRt)), Collections.emptySet()),
        sample_id);
    Record referencerRecord =
        new Record(
            recordId,
            referencer,
            RecordAttributes.empty()
                .putAttribute("attr1", RelationUtils.createRelationString(referencedRt, recordId)));
    recordDao.batchUpsert(
        collectionUuid, referencer, Collections.singletonList(referencerRecord), schema, sample_id);
    recordDao.batchDelete(collectionUuid, referencer, Collections.singletonList(referencerRecord));
    recordDao.batchDelete(collectionUuid, referencedRt, Collections.singletonList(testRecord));
    assertTrue(recordDao.getSingleRecord(collectionUuid, referencer, recordId).isEmpty());
  }

  @Test
  void batchDeleteWithRelationArrays() {
    // Create records to be referenced
    RecordType referencedRt = RecordType.valueOf("referenced");
    List<Record> referencedRecords =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> new Record("id" + i, referencedRt, RecordAttributes.empty()))
            .collect(Collectors.toList());
    recordDao.createRecordType(
        collectionUuid, emptyMap(), referencedRt, RelationCollection.empty(), RECORD_ID);
    recordDao.batchUpsert(collectionUuid, referencedRt, referencedRecords, emptyMap());

    // Create records to do the referencing
    Map<String, DataTypeMapping> schema = Map.of("attr1", ARRAY_OF_RELATION);
    RecordType referencer = RecordType.valueOf("referencer");
    Relation arrayRel = new Relation("attr1", referencedRt);
    recordDao.createRecordType(
        collectionUuid,
        schema,
        referencer,
        new RelationCollection(Collections.emptySet(), Collections.singleton(arrayRel)),
        RECORD_ID);
    List<String> relArr =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(i -> RelationUtils.createRelationString(referencedRt, "id" + i))
            .collect(Collectors.toList());
    List<Record> referencingRecords =
        IntStream.range(0, 3)
            .mapToObj(Integer::toString)
            .map(
                i ->
                    new Record(
                        "id" + i,
                        referencer,
                        RecordAttributes.empty().putAttribute("attr1", relArr)))
            .collect(Collectors.toList());

    recordDao.batchUpsert(collectionUuid, referencer, referencingRecords, schema, RECORD_ID);
    // Normally insert into join is called from the service level, so when working directly with the
    // dao must call it manually
    for (Record rec : referencingRecords) {
      recordDao.insertIntoJoin(
          collectionUuid,
          arrayRel,
          referencer,
          referencedRecords.stream().map(rel -> new RelationValue(rec, rel)).toList());
    }

    // Delete
    recordDao.batchDelete(collectionUuid, referencer, referencingRecords);
    for (Record rec : referencingRecords) {
      assertTrue(recordDao.getSingleRecord(collectionUuid, referencer, rec.getId()).isEmpty());
      assertTrue(
          testDao.getRelationArrayValues(collectionUuid, "attr1", rec, referencedRt).isEmpty());
    }
  }

  @Test
  @Transactional
  void testGetRecordsWithRelations() {
    // Create two records of the same type, one with a value for a relation
    // attribute, the other without
    recordDao.addColumn(collectionUuid, recordType, "foo", STRING);
    recordDao.addColumn(collectionUuid, recordType, "relationAttr", RELATION);
    recordDao.addForeignKeyForReference(recordType, recordType, collectionUuid, "relationAttr");

    String refRecordId = "referencedRecord";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));

    String recordId = "testRecord";
    String reference =
        RelationUtils.createRelationString(
            RecordType.valueOf("testRecordType"), "referencedRecord");
    Record testRecord =
        new Record(recordId, recordType, new RecordAttributes(Map.of("relationAttr", reference)));

    recordDao.batchUpsert(
        collectionUuid, recordType, Collections.singletonList(referencedRecord), emptyMap());
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        List.of(referencedRecord, testRecord),
        Map.of("foo", STRING, "relationAttr", RELATION));

    List<Relation> relations = recordDao.getRelationCols(collectionUuid, recordType);
    assertThat(relations).hasSize(1);

    Record testRecordFetched =
        recordDao.getSingleRecord(collectionUuid, recordType, recordId).get();
    // Relation attribute should be in the form of "terra-wds:/recordType/recordId"
    assertEquals(
        RelationUtils.createRelationString(recordType, refRecordId),
        testRecordFetched.getAttributeValue("relationAttr").toString());

    Record referencedRecordFetched =
        recordDao.getSingleRecord(collectionUuid, recordType, refRecordId).get();
    // Null relation attribute should be null
    assertNull(referencedRecordFetched.getAttributeValue("relationAttr"));
  }

  @Test
  @Transactional
  void testCreateSingleRecord() throws InvalidRelationException {
    recordDao.addColumn(collectionUuid, recordType, "foo", STRING);

    // create record with no attributes
    String recordId = "testRecord";
    Record testRecord = new Record(recordId, recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, recordType, Collections.singletonList(testRecord), emptyMap());

    Record search = recordDao.getSingleRecord(collectionUuid, recordType, recordId).get();
    assertEquals(testRecord, search, "Created record should match entered record");

    // create record with attributes
    String attrId = "recordWithAttr";
    Record recordWithAttr =
        new Record(attrId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        Collections.singletonList(recordWithAttr),
        Map.of("foo", STRING));

    search = recordDao.getSingleRecord(collectionUuid, recordType, attrId).get();
    assertEquals(
        recordWithAttr, search, "Created record with attributes should match entered record");
  }

  @Test
  @Transactional
  void testCreateRecordWithRelations() {
    // make sure columns are in recordType, as this will be taken care of before we
    // get to the dao
    recordDao.addColumn(collectionUuid, recordType, "foo", STRING);

    recordDao.addColumn(collectionUuid, recordType, "testRecordType", RELATION);
    recordDao.addForeignKeyForReference(recordType, recordType, collectionUuid, "testRecordType");

    String refRecordId = "referencedRecord";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    recordDao.batchUpsert(
        collectionUuid, recordType, Collections.singletonList(referencedRecord), emptyMap());

    String recordId = "testRecord";
    String reference =
        RelationUtils.createRelationString(
            RecordType.valueOf("testRecordType"), "referencedRecord");
    Record testRecord =
        new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        Collections.singletonList(testRecord),
        Map.of("foo", STRING, "testRecordType", RELATION));

    Record search = recordDao.getSingleRecord(collectionUuid, recordType, recordId).get();
    assertEquals(testRecord, search, "Created record with references should match entered record");
  }

  @Test
  @Transactional
  void testGetReferenceCols() {
    recordDao.addColumn(collectionUuid, recordType, "referenceCol", STRING);
    recordDao.addForeignKeyForReference(recordType, recordType, collectionUuid, "referenceCol");

    List<Relation> refCols = recordDao.getRelationCols(collectionUuid, recordType);
    assertEquals(1, refCols.size(), "There should be one referenced column");
    assertEquals(
        "referenceCol",
        refCols.get(0).relationColName(),
        "Reference column should be named referenceCol");
  }

  @Test
  @Transactional
  void testDeleteSingleRecord() {
    // add record
    String recordId = "testRecord";
    Record testRecord = new Record(recordId, recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, recordType, Collections.singletonList(testRecord), emptyMap());

    recordDao.deleteSingleRecord(collectionUuid, recordType, "testRecord");

    // make sure record not fetched
    Optional<Record> none = recordDao.getSingleRecord(collectionUuid, recordType, "testRecord");
    assertEquals(Optional.empty(), none, "Deleted record should not be found");
  }

  @Test
  void testDeleteRecords() {
    String recordIdPrefix = "testRecord";
    Record testRecordA = new Record(recordIdPrefix + "A", recordType, RecordAttributes.empty());
    Record testRecordB = new Record(recordIdPrefix + "B", recordType, RecordAttributes.empty());
    Record testRecordC = new Record(recordIdPrefix + "C", recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(testRecordA, testRecordB, testRecordC), emptyMap());

    recordDao.deleteRecords(collectionUuid, recordType, List.of(testRecordB.getId()));

    // make sure record not fetched
    Optional<Record> none =
        recordDao.getSingleRecord(collectionUuid, recordType, testRecordB.getId());
    assertEquals(Optional.empty(), none, "Deleted record should not be found");

    // make sure other records are fetched
    Optional<Record> verifiedRecordA =
        recordDao.getSingleRecord(collectionUuid, recordType, testRecordA.getId());
    assert (verifiedRecordA.isPresent());
    Optional<Record> verifiedRecordC =
        recordDao.getSingleRecord(collectionUuid, recordType, testRecordC.getId());
    assert (verifiedRecordC.isPresent());
  }

  @Test
  @Transactional
  void testDeleteAllRecords() {
    String recordIdPrefix = "testRecord";
    Record testRecordA = new Record(recordIdPrefix + "A", recordType, RecordAttributes.empty());
    Record testRecordB = new Record(recordIdPrefix + "B", recordType, RecordAttributes.empty());
    Record testRecordC = new Record(recordIdPrefix + "C", recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(testRecordA, testRecordB, testRecordC), emptyMap());

    recordDao.deleteAllRecords(
        CollectionId.of(collectionUuid), recordType, Collections.emptyList());

    List<Record> queryRes =
        recordDao.queryForRecords(recordType, 10, 0, "ASC", null, Optional.empty(), collectionUuid);

    assertEquals(0, queryRes.size());
  }

  @Test
  void testDeleteAllRecordsExcept() {
    String recordIdPrefix = "testRecord";
    Record testRecordA = new Record(recordIdPrefix + "A", recordType, RecordAttributes.empty());
    Record testRecordB = new Record(recordIdPrefix + "B", recordType, RecordAttributes.empty());
    Record testRecordC = new Record(recordIdPrefix + "C", recordType, RecordAttributes.empty());
    Record testRecordD = new Record(recordIdPrefix + "D", recordType, RecordAttributes.empty());
    Record testRecordE = new Record(recordIdPrefix + "E", recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        List.of(testRecordA, testRecordB, testRecordC, testRecordD, testRecordE),
        emptyMap());

    recordDao.deleteAllRecords(
        CollectionId.of(collectionUuid),
        recordType,
        List.of(testRecordB.getId(), testRecordD.getId()));

    List<Record> queryRes =
        recordDao.queryForRecords(recordType, 10, 0, "ASC", null, Optional.empty(), collectionUuid);

    assertEquals(2, queryRes.size());
    assertTrue(queryRes.contains(testRecordB));
    assertTrue(queryRes.contains(testRecordD));
  }

  @Test
  void testDeleteRelatedRecord() {
    // make sure columns are in recordType, as this will be taken care of before we
    // get to the dao
    recordDao.addColumn(collectionUuid, recordType, "foo", STRING);

    recordDao.addColumn(
        collectionUuid,
        recordType,
        "testRecordType",
        RELATION,
        RecordType.valueOf("testRecordType"));

    String refRecordId = "referencedRecord";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    recordDao.batchUpsert(
        collectionUuid, recordType, Collections.singletonList(referencedRecord), emptyMap());

    String recordId = "testRecord";
    String reference =
        RelationUtils.createRelationString(
            RecordType.valueOf("testRecordType"), "referencedRecord");
    Record testRecord =
        new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        Collections.singletonList(testRecord),
        Map.of("foo", STRING, "testRecordType", RELATION));

    // Should throw an error
    assertThrows(
        ResponseStatusException.class,
        () -> recordDao.deleteSingleRecord(collectionUuid, recordType, "referencedRecord"),
        "Exception should be thrown when attempting to delete related record");

    // Record should not have been deleted
    assert (recordDao.getSingleRecord(collectionUuid, recordType, "referencedRecord").isPresent());
  }

  @Test
  void testDeleteRecordReferencedInArray() {
    String refRecordId = "referencedRecord1";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    String refRecordId2 = "referencedRecord2";
    Record referencedRecord2 =
        new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(referencedRecord, referencedRecord2), emptyMap());

    // Create record type
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    Relation arrayRelation = new Relation("relArrAttr", recordType);
    Map<String, DataTypeMapping> schema = Map.of("relArrAttr", ARRAY_OF_RELATION);
    recordDao.createRecordType(
        collectionUuid,
        schema,
        relationArrayType,
        new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)),
        RECORD_ID);

    // Create record with relation array
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, refRecordId),
            RelationUtils.createRelationString(recordType, refRecordId2));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    recordDao.batchUpsert(
        collectionUuid,
        relationArrayType,
        Collections.singletonList(recordWithRelationArray),
        schema);

    // Normally insert into join is called from the service level, so when working directly with the
    // dao must call it manually
    recordDao.insertIntoJoin(
        collectionUuid,
        arrayRelation,
        relationArrayType,
        List.of(
            new RelationValue(recordWithRelationArray, referencedRecord),
            new RelationValue(recordWithRelationArray, referencedRecord2)));

    // Should throw an error
    assertThrows(
        ResponseStatusException.class,
        () -> recordDao.deleteSingleRecord(collectionUuid, recordType, "referencedRecord1"),
        "Exception should be thrown when attempting to delete related record");

    // Record should not have been deleted
    assert (recordDao.getSingleRecord(collectionUuid, recordType, "referencedRecord1").isPresent());
  }

  @Test
  void testDeleteRecordWithRelationArray() {
    String refRecordId = "referencedRecord1";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    String refRecordId2 = "referencedRecord2";
    Record referencedRecord2 =
        new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(referencedRecord, referencedRecord2), emptyMap());

    // Create record type
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    Relation arrayRelation = new Relation("relArrAttr", recordType);
    Map<String, DataTypeMapping> schema = Map.of("relArrAttr", ARRAY_OF_RELATION);
    recordDao.createRecordType(
        collectionUuid,
        schema,
        relationArrayType,
        new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)),
        RECORD_ID);

    // Create record with relation array
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, refRecordId),
            RelationUtils.createRelationString(recordType, refRecordId2));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    recordDao.batchUpsert(
        collectionUuid,
        relationArrayType,
        Collections.singletonList(recordWithRelationArray),
        schema);

    // Normally insert into join is called from the service level, so when working directly with the
    // dao must call it manually
    recordDao.insertIntoJoin(
        collectionUuid,
        arrayRelation,
        relationArrayType,
        List.of(
            new RelationValue(recordWithRelationArray, referencedRecord),
            new RelationValue(recordWithRelationArray, referencedRecord2)));

    recordDao.deleteSingleRecord(collectionUuid, relationArrayType, "recordWithRelationArr");

    // Record should have been deleted
    assert (recordDao
        .getSingleRecord(collectionUuid, recordType, "recordWithRelationArr")
        .isEmpty());
    // Check that values are removed from join table
    assertTrue(
        testDao
            .getRelationArrayValues(
                collectionUuid, "relArrAttr", recordWithRelationArray, recordType)
            .isEmpty());
  }

  @Test
  @Transactional
  void testGetAllRecordTypes() {
    List<RecordType> typesList = recordDao.getAllRecordTypes(collectionUuid);
    assertEquals(1, typesList.size());
    assertTrue(typesList.contains(recordType));

    RecordType newRecordType = RecordType.valueOf("newRecordType");
    recordDao.createRecordType(
        collectionUuid, emptyMap(), newRecordType, RelationCollection.empty(), RECORD_ID);

    List<RecordType> newTypesList = recordDao.getAllRecordTypes(collectionUuid);
    assertEquals(2, newTypesList.size());
    assertTrue(newTypesList.contains(recordType));
    assertTrue(newTypesList.contains(newRecordType));
  }

  @Test
  @Transactional
  void testGetAllRecordTypesNoJoins() {
    List<RecordType> typesList = recordDao.getAllRecordTypes(collectionUuid);
    assertEquals(1, typesList.size());
    assertTrue(typesList.contains(recordType));

    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    Relation arrayRelation = new Relation("relArrAttr", recordType);
    recordDao.createRecordType(
        collectionUuid,
        Map.of("relArrAttr", ARRAY_OF_RELATION),
        relationArrayType,
        new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)),
        RECORD_ID);

    List<RecordType> newTypesList = recordDao.getAllRecordTypes(collectionUuid);
    assertEquals(2, newTypesList.size());
    assertTrue(newTypesList.contains(recordType));
    assertTrue(newTypesList.contains(relationArrayType));
  }

  @Test
  @Transactional
  void testCountRecords() {
    // ensure we start with a count of 0 records
    assertEquals(0, recordDao.countRecords(collectionUuid, recordType));
    // insert records and test the count after each insert
    for (int i = 0; i < 10; i++) {
      Record testRecord = new Record("record" + i, recordType, RecordAttributes.empty());
      recordDao.batchUpsert(
          collectionUuid, recordType, Collections.singletonList(testRecord), emptyMap());
      assertEquals(
          i + 1,
          recordDao.countRecords(collectionUuid, recordType),
          "after inserting " + (i + 1) + " records");
    }
  }

  @Test
  @Transactional
  void testRecordExists() {
    assertFalse(recordDao.recordExists(collectionUuid, recordType, "aRecord"));
    recordDao.batchUpsert(
        collectionUuid,
        recordType,
        Collections.singletonList(new Record("aRecord", recordType, RecordAttributes.empty())),
        emptyMap());
    assertTrue(recordDao.recordExists(collectionUuid, recordType, "aRecord"));
  }

  @Test
  @Transactional
  void testDeleteRecordType() {
    // make sure type already exists
    assertTrue(recordDao.recordTypeExists(collectionUuid, recordType));
    recordDao.deleteRecordType(collectionUuid, recordType);
    // make sure type no longer exists
    assertFalse(recordDao.recordTypeExists(collectionUuid, recordType));
  }

  @Test
  void testDeleteRecordTypeWithRelation() {
    RecordType recordTypeName = recordType;
    RecordType referencedType = RecordType.valueOf("referencedType");
    recordDao.createRecordType(
        collectionUuid, emptyMap(), referencedType, RelationCollection.empty(), RECORD_ID);

    recordDao.addColumn(collectionUuid, recordTypeName, "relation", RELATION, referencedType);

    String refRecordId = "referencedRecord";
    Record referencedRecord = new Record(refRecordId, referencedType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, referencedType, Collections.singletonList(referencedRecord), emptyMap());

    String recordId = "testRecord";
    String reference = RelationUtils.createRelationString(referencedType, refRecordId);
    Record testRecord =
        new Record(recordId, recordType, new RecordAttributes(Map.of("relation", reference)));
    recordDao.batchUpsert(
        collectionUuid,
        recordTypeName,
        Collections.singletonList(testRecord),
        Map.of("relation", RELATION));

    // Should throw an error
    assertThrows(
        ResponseStatusException.class,
        () -> recordDao.deleteRecordType(collectionUuid, referencedType),
        "Exception should be thrown when attempting to delete record type with relation");
  }

  @Test
  void testDeleteRecordTypeWithRelationArray() {
    // create records to be referenced
    String refRecordId = "referencedRecord1";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    String refRecordId2 = "referencedRecord2";
    Record referencedRecord2 =
        new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(referencedRecord, referencedRecord2), emptyMap());

    // Create referencing record type
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    Relation arrayRelation = new Relation("relArrAttr", recordType);
    Map<String, DataTypeMapping> schema = Map.of("relArrAttr", ARRAY_OF_RELATION);
    recordDao.createRecordType(
        collectionUuid,
        schema,
        relationArrayType,
        new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)),
        RECORD_ID);

    // Create record with relation array
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, refRecordId),
            RelationUtils.createRelationString(recordType, refRecordId2));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    recordDao.batchUpsert(
        collectionUuid,
        relationArrayType,
        Collections.singletonList(recordWithRelationArray),
        schema);

    // Normally insert into join is called from the service level, so when working directly with the
    // dao must call it manually
    recordDao.insertIntoJoin(
        collectionUuid,
        arrayRelation,
        relationArrayType,
        List.of(
            new RelationValue(recordWithRelationArray, referencedRecord),
            new RelationValue(recordWithRelationArray, referencedRecord2)));

    // Delete record type
    recordDao.deleteRecordType(collectionUuid, relationArrayType);

    // Record table should have been deleted
    assertFalse(recordDao.recordTypeExists(collectionUuid, relationArrayType));

    // check that join table is gone as well
    assertFalse(testDao.joinTableExists(collectionUuid, "relArrAttr", relationArrayType));
  }

  @Test
  @Transactional
  void testRenameAttribute() {
    // Arrange
    RecordType recordTypeWithAttributes = RecordType.valueOf("withAttributes");
    recordDao.createRecordType(
        collectionUuid,
        Map.of("foo", STRING, "bar", STRING),
        recordTypeWithAttributes,
        RelationCollection.empty(),
        PRIMARY_KEY);

    // Act
    recordDao.renameAttribute(collectionUuid, recordTypeWithAttributes, "bar", "baz");

    // Assert
    Set<String> attributeNames =
        Set.copyOf(recordDao.getAllAttributeNames(collectionUuid, recordTypeWithAttributes));
    assertEquals(Set.of(PRIMARY_KEY, "foo", "baz"), attributeNames);
  }

  @ParameterizedTest(name = "returns expression for converting {0} to {1}")
  @MethodSource({
    "stringConversionExpressions",
    "stringArrayConversionExpressions",
    "numberConversionExpressions",
    "numberArrayConversionExpressions",
    "booleanConversionExpressions",
    "booleanArrayConversionExpressions",
    "dateConversionExpressions",
    "dateArrayConversionExpressions",
    "datetimeConversionExpressions",
    "datetimeArrayConversionExpressions"
  })
  void testGetPostgresTypeConversionExpression(
      DataTypeMapping dataType, DataTypeMapping newDataType, String expectedExpression) {
    // Act
    String actualExpression =
        recordDao.getPostgresTypeConversionExpression("attr", dataType, newDataType);

    // Assert
    assertEquals(expectedExpression, actualExpression);
  }

  static Stream<Arguments> stringConversionExpressions() {
    return Stream.of(
        // Number
        args(STRING, NUMBER, "\"attr\"::numeric"),
        // Boolean
        args(STRING, BOOLEAN, "\"attr\"::boolean"),
        // Date
        args(STRING, DATE, "\"attr\"::date"),
        // Datetime
        args(STRING, DATE_TIME, "\"attr\"::timestamp with time zone"),
        // String array
        args(STRING, ARRAY_OF_STRING, "array_append('{}', \"attr\")::text[]"),
        // Number array
        args(STRING, ARRAY_OF_NUMBER, "array_append('{}', \"attr\")::numeric[]"),
        // Boolean array
        args(STRING, ARRAY_OF_BOOLEAN, "array_append('{}', \"attr\")::boolean[]"),
        // Date array
        args(STRING, ARRAY_OF_DATE, "array_append('{}', \"attr\")::date[]"),
        // Datetime array
        args(
            STRING,
            ARRAY_OF_DATE_TIME,
            "array_append('{}', \"attr\")::timestamp with time zone[]"));
  }

  static Stream<Arguments> stringArrayConversionExpressions() {
    return Stream.of(
        // Number array
        args(ARRAY_OF_STRING, ARRAY_OF_NUMBER, "\"attr\"::numeric[]"),
        // Boolean array
        args(ARRAY_OF_STRING, ARRAY_OF_BOOLEAN, "\"attr\"::boolean[]"),
        // Date array
        args(ARRAY_OF_STRING, ARRAY_OF_DATE, "\"attr\"::date[]"),
        // Datetime array
        args(ARRAY_OF_STRING, ARRAY_OF_DATE_TIME, "\"attr\"::timestamp with time zone[]"));
  }

  static Stream<Arguments> numberConversionExpressions() {
    return Stream.of(
        // String
        args(NUMBER, STRING, "\"attr\"::text"),
        // Boolean
        args(NUMBER, BOOLEAN, "\"attr\"::int::boolean"),
        // Date
        args(NUMBER, DATE, "to_timestamp(\"attr\")::date"),
        // Datetime
        args(NUMBER, DATE_TIME, "to_timestamp(\"attr\")::timestamp with time zone"),
        // String array
        args(NUMBER, ARRAY_OF_STRING, "array_append('{}', \"attr\")::text[]"),
        // Number array
        args(NUMBER, ARRAY_OF_NUMBER, "array_append('{}', \"attr\")::numeric[]"),
        // Boolean array
        args(NUMBER, ARRAY_OF_BOOLEAN, "array_append('{}', \"attr\")::int[]::boolean[]"),
        // Date array
        args(NUMBER, ARRAY_OF_DATE, "array_append('{}', to_timestamp(\"attr\"))::date[]"),
        // Datetime array
        args(
            NUMBER,
            ARRAY_OF_DATE_TIME,
            "array_append('{}', to_timestamp(\"attr\"))::timestamp with time zone[]"));
  }

  static Stream<Arguments> numberArrayConversionExpressions() {
    return Stream.of(
        // String array
        args(ARRAY_OF_NUMBER, ARRAY_OF_STRING, "\"attr\"::text[]"),
        // Boolean array
        args(ARRAY_OF_NUMBER, ARRAY_OF_BOOLEAN, "\"attr\"::int[]::boolean[]"),
        // Date array
        args(
            ARRAY_OF_NUMBER,
            ARRAY_OF_DATE,
            "(sys_wds.convert_array_of_numbers_to_timestamps(\"attr\"))::date[]"),
        // Datetime array
        args(
            ARRAY_OF_NUMBER,
            ARRAY_OF_DATE_TIME,
            "(sys_wds.convert_array_of_numbers_to_timestamps(\"attr\"))::timestamp with time zone[]"));
  }

  static Stream<Arguments> booleanConversionExpressions() {
    return Stream.of(
        // String
        args(BOOLEAN, STRING, "\"attr\"::text"),
        // Number
        args(BOOLEAN, NUMBER, "\"attr\"::int::numeric"),
        // String array
        args(BOOLEAN, ARRAY_OF_STRING, "array_append('{}', \"attr\")::text[]"),
        // Number array
        args(BOOLEAN, ARRAY_OF_NUMBER, "array_append('{}', \"attr\")::int[]::numeric[]"),
        // Boolean array
        args(BOOLEAN, ARRAY_OF_BOOLEAN, "array_append('{}', \"attr\")::boolean[]"));
  }

  static Stream<Arguments> booleanArrayConversionExpressions() {
    return Stream.of(
        // String array
        args(ARRAY_OF_BOOLEAN, ARRAY_OF_STRING, "\"attr\"::text[]"),
        // Number array
        args(ARRAY_OF_BOOLEAN, ARRAY_OF_NUMBER, "\"attr\"::int[]::numeric[]"));
  }

  static Stream<Arguments> dateConversionExpressions() {
    return Stream.of(
        // String
        args(DATE, STRING, "\"attr\"::text"),
        // Number
        args(DATE, NUMBER, "extract(epoch from \"attr\")::bigint::numeric"),
        // Datetime
        args(DATE, DATE_TIME, "\"attr\"::timestamp with time zone"),
        // String array
        args(DATE, ARRAY_OF_STRING, "array_append('{}', \"attr\")::text[]"),
        // Number array
        args(
            DATE,
            ARRAY_OF_NUMBER,
            "array_append('{}', extract(epoch from \"attr\")::bigint)::numeric[]"),
        // Date array
        args(DATE, ARRAY_OF_DATE, "array_append('{}', \"attr\")::date[]"),
        // Datetime array
        args(DATE, ARRAY_OF_DATE_TIME, "array_append('{}', \"attr\")::timestamp with time zone[]"));
  }

  static Stream<Arguments> dateArrayConversionExpressions() {
    return Stream.of(
        // String array
        args(ARRAY_OF_DATE, ARRAY_OF_STRING, "\"attr\"::text[]"),
        // Number array
        args(
            ARRAY_OF_DATE,
            ARRAY_OF_NUMBER,
            "(sys_wds.convert_array_of_timestamps_to_numbers(\"attr\")::bigint[])::numeric[]"),
        // Datetime array
        args(ARRAY_OF_DATE, ARRAY_OF_DATE_TIME, "\"attr\"::timestamp with time zone[]"));
  }

  static Stream<Arguments> datetimeConversionExpressions() {
    return Stream.of(
        // String
        args(DATE_TIME, STRING, "\"attr\"::text"),
        // Number
        args(DATE_TIME, NUMBER, "extract(epoch from \"attr\")::numeric"),
        // Date
        args(DATE_TIME, DATE, "\"attr\"::date"),
        // String array
        args(DATE_TIME, ARRAY_OF_STRING, "array_append('{}', \"attr\")::text[]"),
        // Number array
        args(
            DATE_TIME,
            ARRAY_OF_NUMBER,
            "array_append('{}', extract(epoch from \"attr\"))::numeric[]"),
        // Date array
        args(DATE_TIME, ARRAY_OF_DATE, "array_append('{}', \"attr\")::date[]"),
        // Datetime array
        args(
            DATE_TIME,
            ARRAY_OF_DATE_TIME,
            "array_append('{}', \"attr\")::timestamp with time zone[]"));
  }

  static Stream<Arguments> datetimeArrayConversionExpressions() {
    return Stream.of(
        // String array
        args(ARRAY_OF_DATE_TIME, ARRAY_OF_STRING, "\"attr\"::text[]"),
        // Number array
        args(
            ARRAY_OF_DATE_TIME,
            ARRAY_OF_NUMBER,
            "(sys_wds.convert_array_of_timestamps_to_numbers(\"attr\"))::numeric[]"),
        // Date array
        args(ARRAY_OF_DATE_TIME, ARRAY_OF_DATE, "\"attr\"::date[]"));
  }

  /** Simple helper to provide an even terser shorthand */
  private static Arguments args(Object... arguments) {
    return Arguments.of(arguments);
  }

  @Test
  @Transactional
  void testDeleteAttribute() {
    // Arrange
    RecordType recordTypeWithAttributes = RecordType.valueOf("withAttributes");
    recordDao.createRecordType(
        collectionUuid,
        Map.of("attr1", STRING, "attr2", STRING),
        recordTypeWithAttributes,
        RelationCollection.empty(),
        PRIMARY_KEY);

    // Act
    recordDao.deleteAttribute(collectionUuid, recordTypeWithAttributes, "attr2");

    // Assert
    Set<String> attributeNames =
        Set.copyOf(recordDao.getAllAttributeNames(collectionUuid, recordTypeWithAttributes));
    assertEquals(Set.of(PRIMARY_KEY, "attr1"), attributeNames);
  }

  @Test
  @Transactional
  void testCreateRelationJoinTable() {
    RecordType secondRecordType = RecordType.valueOf("secondRecordType");
    recordDao.createRecordType(
        collectionUuid, emptyMap(), secondRecordType, RelationCollection.empty(), RECORD_ID);

    recordDao.createRelationJoinTable(collectionUuid, "refArray", secondRecordType, recordType);

    List<Relation> relationArrays =
        recordDao.getRelationArrayCols(collectionUuid, secondRecordType);
    assertEquals(1, relationArrays.size());
    assertTrue(relationArrays.contains(new Relation("refArray", recordType)));
    assertTrue(testDao.joinTableExists(collectionUuid, "refArray", secondRecordType));
  }

  @Test
  @Transactional
  void testCreateRecordTypeWithRelationArray() {
    RecordType relationarrayType = RecordType.valueOf("relationArrayType");
    Relation singleRelation = new Relation("refAttr", recordType);
    Relation arrayRelation = new Relation("relArrAttr", recordType);
    recordDao.createRecordType(
        collectionUuid,
        Map.of("stringAttr", STRING, "refAttr", RELATION, "relArrAttr", ARRAY_OF_RELATION),
        relationarrayType,
        new RelationCollection(Set.of(singleRelation), Set.of(arrayRelation)),
        RECORD_ID);

    Map<String, DataTypeMapping> schema =
        recordDao.getExistingTableSchemaLessPrimaryKey(collectionUuid, relationarrayType);
    assertEquals(3, schema.size());
    assertEquals(STRING, schema.get("stringAttr"));
    assertEquals(RELATION, schema.get("refAttr"));
    assertEquals(ARRAY_OF_RELATION, schema.get("relArrAttr"));
    List<Relation> relationCols = recordDao.getRelationCols(collectionUuid, relationarrayType);
    assertEquals(List.of(singleRelation), relationCols);
    List<Relation> relationArrayCols =
        recordDao.getRelationArrayCols(collectionUuid, relationarrayType);
    assertEquals(List.of(arrayRelation), relationArrayCols);
    assertTrue(testDao.joinTableExists(collectionUuid, "relArrAttr", relationarrayType));
  }

  @Test
  @Transactional
  void testCreateAndGetRecordWithRelationArray() {
    // add some records to be relations
    String refRecordId = "referencedRecord1";
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    String refRecordId2 = "referencedRecord12";
    Record referencedRecord2 =
        new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(referencedRecord, referencedRecord2), emptyMap());

    // Create record type
    RecordType relationArrayType = RecordType.valueOf("relationArrayType");
    Relation arrayRelation = new Relation("relArrAttr", recordType);
    Map<String, DataTypeMapping> schema =
        Map.of("stringAttr", STRING, "refAttr", RELATION, "relArrAttr", ARRAY_OF_RELATION);
    recordDao.createRecordType(
        collectionUuid,
        schema,
        relationArrayType,
        new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)),
        RECORD_ID);

    // Create record with relation array
    String relArrId = "recordWithRelationArr";
    List<String> relArr =
        List.of(
            RelationUtils.createRelationString(recordType, refRecordId),
            RelationUtils.createRelationString(recordType, refRecordId2));
    Record recordWithRelationArray =
        new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
    recordDao.batchUpsert(
        collectionUuid,
        relationArrayType,
        Collections.singletonList(recordWithRelationArray),
        schema);

    Map<String, DataTypeMapping> createdSchema =
        recordDao.getExistingTableSchemaLessPrimaryKey(collectionUuid, relationArrayType);
    assertEquals(3, createdSchema.size());
    List<Relation> relationArrayCols =
        recordDao.getRelationArrayCols(collectionUuid, relationArrayType);
    assertEquals(List.of(arrayRelation), relationArrayCols);
    Record rec = recordDao.getSingleRecord(collectionUuid, relationArrayType, relArrId).get();
    assertNotNull(rec);
    String[] actualAttrValue =
        assertInstanceOf(String[].class, rec.getAttributeValue("relArrAttr"));
    assertIterableEquals(relArr, Arrays.asList(actualAttrValue));

    // The purpose of inserting in to the join is to make sure foreign keys are consistent
    // So we need to make sure no error is thrown
    assertDoesNotThrow(
        () ->
            recordDao.insertIntoJoin(
                collectionUuid,
                arrayRelation,
                relationArrayType,
                List.of(
                    new RelationValue(rec, referencedRecord),
                    new RelationValue(rec, referencedRecord2))));
    assertEquals(
        List.of(refRecordId, refRecordId2),
        testDao.getRelationArrayValues(collectionUuid, "relArrAttr", rec, recordType));
  }

  @Test
  @Transactional
  void testGetRelationArrayColumns() {
    // Add relation array columns to a type
    RecordType relationarrayType = RecordType.valueOf("relationArrayType");
    Relation arrayRelation1 = new Relation("relArr1", recordType);
    Relation arrayRelation2 = new Relation("relArr2", recordType);
    recordDao.createRecordType(
        collectionUuid,
        Map.of("relArr1", ARRAY_OF_RELATION, "relArr2", ARRAY_OF_RELATION),
        relationarrayType,
        new RelationCollection(Collections.emptySet(), Set.of(arrayRelation1, arrayRelation2)),
        RECORD_ID);

    List<Relation> cols = recordDao.getRelationArrayCols(collectionUuid, relationarrayType);
    assertEquals(2, cols.size());
    assertTrue(cols.contains(arrayRelation1));
    assertTrue(cols.contains(arrayRelation2));
  }

  @Test
  @Transactional
  void testRemoveFromJoin() {
    // create records to reference in join table
    String fromRecordId = "fromRecord1";
    Record fromRecord = new Record(fromRecordId, recordType, RecordAttributes.empty());
    String fromRecordId2 = "fromRecord2";
    Record fromRecord2 = new Record(fromRecordId2, recordType, RecordAttributes.empty());
    String fromRecordId3 = "fromRecord3";
    Record fromRecord3 = new Record(fromRecordId3, recordType, RecordAttributes.empty());
    recordDao.batchUpsert(
        collectionUuid, recordType, List.of(fromRecord, fromRecord2, fromRecord3), emptyMap());

    RecordType toType = RecordType.valueOf("toType");
    recordDao.createRecordType(
        collectionUuid, emptyMap(), toType, RelationCollection.empty(), RECORD_ID);
    String toRecordId = "toRecord1";
    Record toRecord = new Record(toRecordId, toType, RecordAttributes.empty());
    String toRecordId2 = "toRecord2";
    Record toRecord2 = new Record(toRecordId2, toType, RecordAttributes.empty());
    recordDao.batchUpsert(collectionUuid, toType, List.of(toRecord, toRecord2), emptyMap());

    // create join table
    recordDao.createRelationJoinTable(collectionUuid, "referenceArray", recordType, toType);

    // insert into join table
    Relation rel = new Relation("referenceArray", toType);
    recordDao.insertIntoJoin(
        collectionUuid,
        rel,
        recordType,
        List.of(
            new RelationValue(fromRecord, toRecord), new RelationValue(fromRecord, toRecord2),
            new RelationValue(fromRecord2, toRecord), new RelationValue(fromRecord2, toRecord2),
            new RelationValue(fromRecord3, toRecord), new RelationValue(fromRecord3, toRecord2)));

    // Check that values are in join table
    List<String> joinVals1 =
        testDao.getRelationArrayValues(collectionUuid, "referenceArray", fromRecord, toType);
    assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals1);
    List<String> joinVals2 =
        testDao.getRelationArrayValues(collectionUuid, "referenceArray", fromRecord2, toType);
    assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals2);
    List<String> joinVals3 =
        testDao.getRelationArrayValues(collectionUuid, "referenceArray", fromRecord3, toType);
    assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals3);

    // remove from join table
    recordDao.removeFromJoin(collectionUuid, rel, recordType, List.of(fromRecordId, fromRecordId3));

    // Make sure values have been removed
    joinVals1 =
        testDao.getRelationArrayValues(collectionUuid, "referenceArray", fromRecord, toType);
    assert (joinVals1.isEmpty());
    joinVals3 =
        testDao.getRelationArrayValues(collectionUuid, "referenceArray", fromRecord3, toType);
    assert (joinVals3.isEmpty());
    // But not other values
    joinVals2 =
        testDao.getRelationArrayValues(collectionUuid, "referenceArray", fromRecord2, toType);
    assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals2);
  }

  @ParameterizedTest(name = "for datatype {0}")
  @EnumSource(
      value = DataTypeMapping.class,
      names = {
        "ARRAY_OF_STRING",
        "ARRAY_OF_FILE",
        "ARRAY_OF_RELATION",
        "ARRAY_OF_DATE",
        "ARRAY_OF_DATE_TIME",
        "ARRAY_OF_NUMBER",
        "EMPTY_ARRAY"
      })
  void testOnlyNullsInList(DataTypeMapping typeMapping) {
    List<?> input = new ArrayList<>();
    input.add(null);
    input.add(null);
    input.add(null);
    String[] expected = new String[] {null, null, null};
    Object[] actual = recordDao.getListAsArray(input, typeMapping);

    assertArrayEquals(expected, actual);
  }

  @Test
  void testSomeNullsInStringList() {
    List<String> input = new ArrayList<>();
    input.add(null);
    input.add("foo");
    input.add(null);
    input.add("bar");
    input.add(null);
    String[] expected = new String[] {null, "foo", null, "bar", null};
    Object[] actual = recordDao.getListAsArray(input, ARRAY_OF_STRING);

    assertArrayEquals(expected, actual);
  }

  @Test
  void testSomeNullsInNumberList() {
    List<Double> input = new ArrayList<>();
    input.add(null);
    input.add(3.14);
    input.add(null);
    input.add(789d);
    input.add(null);
    String[] expected = new String[] {null, "3.14", null, "789.0", null};
    Object[] actual = recordDao.getListAsArray(input, ARRAY_OF_NUMBER);

    assertArrayEquals(expected, actual);
  }
}
