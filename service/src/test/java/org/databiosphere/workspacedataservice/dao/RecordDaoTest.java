package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.RelationValue;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordDaoTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(RecordDaoTest.class);

	private static final String PRIMARY_KEY = "row_id";
	@Autowired
	RecordDao recordDao;

	@Autowired
	InstanceDao instanceDao;

	UUID instanceId;
	RecordType recordType;

	@Autowired
	TestDao testDao;

	@Autowired
	NamedParameterJdbcTemplate namedTemplate;

	@Autowired
	@Qualifier("streamingDs")
	NamedParameterJdbcTemplate templateForStreaming;

	@Autowired
	DataTypeInferer dataTypeInferer;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	CachedQueryDao cachedQueryDao;

	@Value("${twds.instance.workspace-id}")
	String workspaceId;

	@BeforeEach
	void setUp() {
		instanceId = UUID.randomUUID();
		recordType = RecordType.valueOf("testRecordType");

		instanceDao.createSchema(instanceId);
		recordDao.createRecordType(instanceId, Collections.emptyMap(), recordType, new RelationCollection(Collections.emptySet(), Collections.emptySet()), PRIMARY_KEY);
	}

	@AfterEach
	void cleanUp(){
		instanceDao.dropSchema(instanceId);
	}

//	@Test
//	void defaultSchemaIsCreated() {
//		LOGGER.info("Default workspace id loaded as {}", workspaceId);
//		UUID defaultInstanceId = UUID.fromString(workspaceId);
//		assertTrue(instanceDao.instanceSchemaExists(defaultInstanceId));
//	}

	/**
	 * This test is somewhat fuzzy. Because we use a persistent db for our unit tests,
	 * and because other tests in this codebase don't properly clean up their instances
	 * and/or human users don't clean up their instances, we can't effectively test
	 * the exact value that should be returned from list-instances. But we can test
	 * that the return value changes in the ways we expect when we create/delete instances.
	 */
	@Test
	void listInstances() {
		// get the list of instances in this DB
		List<UUID> actualInitialSchemas = instanceDao.listInstanceSchemas();

		// check that the default schema exists - see also defaultSchemaIsCreated() above
//		UUID defaultInstanceId = UUID.fromString(workspaceId);
//		assertTrue(actualInitialSchemas.contains(defaultInstanceId),
//				"initial schema list should contain default instance");

		// generate some new UUIDs
		List<UUID> someInstancesToCreate = IntStream.range(0, 5)
				.mapToObj(i -> UUID.randomUUID())
				.toList();

		// check that the new UUIDs do not exist in our instances list yet.
		someInstancesToCreate.forEach( inst ->
				assertFalse(actualInitialSchemas.contains(inst),
						"initial schema list should not contain brand new UUIDs"));

		// create the instances
		someInstancesToCreate.forEach( inst ->
				instanceDao.createSchema(inst));

		// get the list of instances again
		List<UUID> actualSchemasAfterCreation = instanceDao.listInstanceSchemas();

		// check that the new UUIDs do exist in our instances list.
		someInstancesToCreate.forEach( inst ->
				assertTrue(actualSchemasAfterCreation.contains(inst),
						"schema list after creation step should contain the new UUIDs"));

		// delete the new instances
		someInstancesToCreate.forEach( inst ->
				instanceDao.dropSchema(inst));

		// get the list of instances again
		List<UUID> actualSchemasAfterDeletion = instanceDao.listInstanceSchemas();

		// check that the new UUIDs do not exist in our instances list, now that we've deleted them
		someInstancesToCreate.forEach( inst ->
				assertFalse(actualSchemasAfterDeletion.contains(inst),
						"schema list after deletion step should not contain the new UUIDs"));

		// at this point, the "after deletion" list and the "initial" list should be the same
		assertIterableEquals(actualInitialSchemas, actualSchemasAfterDeletion);
	}

	@Test
	void listNonUuidInstances() {
		List<UUID> initialInstances = instanceDao.listInstanceSchemas();
		namedTemplate.getJdbcTemplate().update("create schema if not exists notAUuid");
		List<UUID> testableInstances = instanceDao.listInstanceSchemas(); // should not throw
		// second call should filter out the non-uuid
		assertIterableEquals(initialInstances, testableInstances);
		// cleanup
		namedTemplate.getJdbcTemplate().update("drop schema if exists notAUuid");
	}

	@Test
	@Transactional
	void testGetSingleRecord() {
		// add record
		String recordId = "testRecord";
		Record testRecord = new Record(recordId, recordType, RecordAttributes.empty());
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord), new HashMap<>());

		// make sure record is fetched
		Record search = recordDao.getSingleRecord(instanceId, recordType, recordId).get();
		assertEquals(testRecord, search);

		// nonexistent record should be null
		Optional<Record> none = recordDao.getSingleRecord(instanceId, recordType, "noRecord");
		assertEquals(none, Optional.empty());
	}

	@Test
	void deleteAndQueryFunkyPrimaryKeyValues(){
		RecordType funkyPk = RecordType.valueOf("funkyPk");
		String sample_id = "Sample ID";
		String recordId = "1199";
		Record testRecord = new Record(recordId, funkyPk, RecordAttributes.empty());
		recordDao.createRecordType(instanceId, Map.of("attr1", DataTypeMapping.STRING), funkyPk, new RelationCollection(Collections.emptySet(),Collections.emptySet()), sample_id);
		recordDao.batchUpsert(instanceId, funkyPk, Collections.singletonList(testRecord), Collections.emptyMap(), sample_id);
		List<Record> queryRes = recordDao.queryForRecords(funkyPk, 10, 0, "ASC", null, instanceId);
		assertEquals(1, queryRes.size());
		assertTrue(recordDao.recordExists(instanceId, funkyPk, recordId));
		assertTrue(recordDao.getSingleRecord(instanceId, funkyPk, recordId).isPresent());
		assertTrue(recordDao.deleteSingleRecord(instanceId, funkyPk, recordId));
	}

	@Test
	void batchDeleteAndTestRelationsFunkyPrimaryKeyValues(){
		RecordType referencedRt = RecordType.valueOf("referenced");
		String sample_id = "Sample ID";
		String recordId = "1199";
		Record testRecord = new Record(recordId, referencedRt, RecordAttributes.empty());
		Map<String, DataTypeMapping> schema = Map.of("attr1", DataTypeMapping.RELATION);
		recordDao.createRecordType(instanceId, schema, referencedRt, new RelationCollection(Collections.emptySet(),Collections.emptySet()), sample_id);
		recordDao.batchUpsert(instanceId, referencedRt, Collections.singletonList(testRecord), Collections.emptyMap(), sample_id);
		RecordType referencer = RecordType.valueOf("referencer");
		recordDao.createRecordType(instanceId, schema, referencer,
				new RelationCollection(Collections.singleton(new Relation("attr1", referencedRt)), Collections.emptySet()), sample_id);
		Record referencerRecord = new Record(recordId, referencer,
				RecordAttributes.empty().putAttribute("attr1", RelationUtils.createRelationString(referencedRt, recordId)));
		recordDao.batchUpsert(instanceId, referencer, Collections.singletonList(referencerRecord), schema, sample_id);
		recordDao.batchDelete(instanceId, referencer, Collections.singletonList(referencerRecord));
		recordDao.batchDelete(instanceId, referencedRt, Collections.singletonList(testRecord));
		assertTrue(recordDao.getSingleRecord(instanceId, referencer, recordId).isEmpty());
	}

	@Test
	void batchDeleteWithRelationArrays(){
		//Create records to be referenced
		RecordType referencedRt = RecordType.valueOf("referenced");
		List<Record> referencedRecords = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> new Record("id" + i, referencedRt, RecordAttributes.empty())).collect(Collectors.toList());
		recordDao.createRecordType(instanceId, Collections.emptyMap(), referencedRt, new RelationCollection(Collections.emptySet(),Collections.emptySet()), RECORD_ID);
		recordDao.batchUpsert(instanceId, referencedRt, referencedRecords, Collections.emptyMap());

		//Create records to do the referencing
		Map<String, DataTypeMapping> schema = Map.of("attr1", DataTypeMapping.ARRAY_OF_RELATION);
		RecordType referencer = RecordType.valueOf("referencer");
		Relation arrayRel = new Relation("attr1", referencedRt);
		recordDao.createRecordType(instanceId, schema, referencer,
				new RelationCollection(Collections.emptySet(), Collections.singleton(arrayRel)), RECORD_ID);
		List<String> relArr = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> RelationUtils.createRelationString(referencedRt, "id" + i)).collect(Collectors.toList());
		List<Record> referencingRecords = IntStream.range(0,3).mapToObj(Integer::toString).map(i -> new Record("id" + i, referencer, RecordAttributes.empty().putAttribute("attr1", relArr))).collect(Collectors.toList());

		recordDao.batchUpsert(instanceId, referencer, referencingRecords, schema, RECORD_ID);
		//Normally insert into join is called from the service level, so when working directly with the dao must call it manually
		for (Record rec : referencingRecords){
			recordDao.insertIntoJoin(instanceId, arrayRel, referencer, referencedRecords.stream().map(rel -> new RelationValue(rec, rel)).toList());
		}

		//Delete
		recordDao.batchDelete(instanceId, referencer, referencingRecords);
		for (Record rec : referencingRecords){
			assertTrue(recordDao.getSingleRecord(instanceId, referencer, rec.getId()).isEmpty());
			assertTrue(testDao.getRelationArrayValues(instanceId, "attr1", rec, referencedRt).isEmpty());
		}
	}

	@Test
	@Transactional
	void testGetRecordsWithRelations() {
		// Create two records of the same type, one with a value for a relation
		// attribute, the other without
		recordDao.addColumn(instanceId, recordType, "foo", DataTypeMapping.STRING);
		recordDao.addColumn(instanceId, recordType, "relationAttr", DataTypeMapping.RELATION);
		recordDao.addForeignKeyForReference(recordType, recordType, instanceId, "relationAttr");

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("relationAttr", reference)));

		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(referencedRecord), new HashMap<>());
		recordDao.batchUpsert(instanceId, recordType, List.of(referencedRecord, testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "relationAttr", DataTypeMapping.RELATION)));

		List<Relation> relations = recordDao.getRelationCols(instanceId, recordType);

		Record testRecordFetched = recordDao.getSingleRecord(instanceId, recordType, recordId).get();
		// Relation attribute should be in the form of "terra-wds:/recordType/recordId"
		assertEquals(RelationUtils.createRelationString(recordType, refRecordId),
				testRecordFetched.getAttributeValue("relationAttr").toString());

		Record referencedRecordFetched = recordDao.getSingleRecord(instanceId, recordType, refRecordId)
				.get();
		// Null relation attribute should be null
		assertNull(referencedRecordFetched.getAttributeValue("relationAttr"));
	}

	@Test
	@Transactional
	void testCreateSingleRecord() throws InvalidRelationException {
		recordDao.addColumn(instanceId, recordType, "foo", DataTypeMapping.STRING);

		// create record with no attributes
		String recordId = "testRecord";
		Record testRecord = new Record(recordId, recordType, RecordAttributes.empty());
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord), new HashMap<>());

		Record search = recordDao.getSingleRecord(instanceId, recordType, recordId).get();
		assertEquals(testRecord, search, "Created record should match entered record");

		// create record with attributes
		String attrId = "recordWithAttr";
		Record recordWithAttr = new Record(attrId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(recordWithAttr),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING)));

		search = recordDao.getSingleRecord(instanceId, recordType, attrId).get();
		assertEquals(recordWithAttr, search, "Created record with attributes should match entered record");
	}

	@Test
	@Transactional
	void testCreateRecordWithRelations() {
		// make sure columns are in recordType, as this will be taken care of before we
		// get to the dao
		recordDao.addColumn(instanceId, recordType, "foo", DataTypeMapping.STRING);

		recordDao.addColumn(instanceId, recordType, "testRecordType", DataTypeMapping.RELATION);
		recordDao.addForeignKeyForReference(recordType, recordType, instanceId, "testRecordType");

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(referencedRecord), new HashMap<>());

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "testRecordType", DataTypeMapping.RELATION)));

		Record search = recordDao
				.getSingleRecord(instanceId, recordType, recordId)
				.get();
		assertEquals(testRecord, search, "Created record with references should match entered record");
	}

	@Test
	@Transactional
	void testGetReferenceCols() {
		recordDao.addColumn(instanceId, recordType, "referenceCol", DataTypeMapping.STRING);
		recordDao.addForeignKeyForReference(recordType, recordType, instanceId, "referenceCol");

		List<Relation> refCols = recordDao.getRelationCols(instanceId, recordType);
		assertEquals(1, refCols.size(), "There should be one referenced column");
		assertEquals("referenceCol", refCols.get(0).relationColName(), "Reference column should be named referenceCol");
	}

	@Test
	@Transactional
	void testDeleteSingleRecord() {
		// add record
		String recordId = "testRecord";
		Record testRecord = new Record(recordId, recordType, RecordAttributes.empty());
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord), new HashMap<>());

		recordDao.deleteSingleRecord(instanceId, recordType, "testRecord");

		// make sure record not fetched
		Optional<Record> none = recordDao.getSingleRecord(instanceId, recordType, "testRecord");
		assertEquals(Optional.empty(), none, "Deleted record should not be found");
	}

	@Test
	void testDeleteRelatedRecord() {
		// make sure columns are in recordType, as this will be taken care of before we
		// get to the dao
		recordDao.addColumn(instanceId, recordType, "foo", DataTypeMapping.STRING);

		recordDao.addColumn(instanceId, recordType, "testRecordType", DataTypeMapping.RELATION,
				RecordType.valueOf("testRecordType"));

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(referencedRecord), new HashMap<>());

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "testRecordType", DataTypeMapping.RELATION)));

		// Should throw an error
		assertThrows(ResponseStatusException.class, () -> {
			recordDao.deleteSingleRecord(instanceId, recordType, "referencedRecord");
		}, "Exception should be thrown when attempting to delete related record");

		//Record should not have been deleted
		assert(recordDao.getSingleRecord(instanceId, recordType, "referencedRecord").isPresent());

	}

	@Test
	void testDeleteRecordReferencedInArray() {
		String refRecordId = "referencedRecord1";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		String refRecordId2 = "referencedRecord2";
		Record referencedRecord2 = new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
		recordDao.batchUpsert(instanceId, recordType, List.of(referencedRecord, referencedRecord2), new HashMap<>());

		//Create record type
		RecordType relationArrayType = RecordType.valueOf("relationArrayType");
		Relation arrayRelation = new Relation("relArrAttr", recordType);
		Map<String, DataTypeMapping> schema = Map.of("relArrAttr", DataTypeMapping.ARRAY_OF_RELATION);
		recordDao.createRecordType(instanceId, schema, relationArrayType,
				new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)), RECORD_ID);

		//Create record with relation array
		String relArrId = "recordWithRelationArr";
		List<String> relArr = List.of(RelationUtils.createRelationString(recordType, refRecordId), RelationUtils.createRelationString(recordType, refRecordId2));
		Record recordWithRelationArray = new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
		recordDao.batchUpsert(instanceId, relationArrayType, Collections.singletonList(recordWithRelationArray), schema);

		//Normally insert into join is called from the service level, so when working directly with the dao must call it manually
		recordDao.insertIntoJoin(instanceId, arrayRelation, relationArrayType, List.of(new RelationValue(recordWithRelationArray, referencedRecord), new RelationValue(recordWithRelationArray, referencedRecord2)));

		// Should throw an error
		assertThrows(ResponseStatusException.class, () -> {
			recordDao.deleteSingleRecord(instanceId, recordType, "referencedRecord1");
		}, "Exception should be thrown when attempting to delete related record");

		//Record should not have been deleted
		assert(recordDao.getSingleRecord(instanceId, recordType, "referencedRecord1").isPresent());
	}

	@Test
	void testDeleteRecordWithRelationArray() {
		String refRecordId = "referencedRecord1";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		String refRecordId2 = "referencedRecord2";
		Record referencedRecord2 = new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
		recordDao.batchUpsert(instanceId, recordType, List.of(referencedRecord, referencedRecord2), new HashMap<>());

		//Create record type
		RecordType relationArrayType = RecordType.valueOf("relationArrayType");
		Relation arrayRelation = new Relation("relArrAttr", recordType);
		Map<String, DataTypeMapping> schema = Map.of("relArrAttr", DataTypeMapping.ARRAY_OF_RELATION);
		recordDao.createRecordType(instanceId, schema, relationArrayType,
				new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)), RECORD_ID);

		//Create record with relation array
		String relArrId = "recordWithRelationArr";
		List<String> relArr = List.of(RelationUtils.createRelationString(recordType, refRecordId), RelationUtils.createRelationString(recordType, refRecordId2));
		Record recordWithRelationArray = new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
		recordDao.batchUpsert(instanceId, relationArrayType, Collections.singletonList(recordWithRelationArray), schema);

		//Normally insert into join is called from the service level, so when working directly with the dao must call it manually
		recordDao.insertIntoJoin(instanceId, arrayRelation, relationArrayType, List.of(new RelationValue(recordWithRelationArray, referencedRecord), new RelationValue(recordWithRelationArray, referencedRecord2)));

		recordDao.deleteSingleRecord(instanceId, relationArrayType, "recordWithRelationArr");

		//Record should have been deleted
		assert(recordDao.getSingleRecord(instanceId, recordType, "recordWithRelationArr").isEmpty());
		//Check that values are removed from join table
		assertTrue(testDao.getRelationArrayValues(instanceId, "relArrAttr", recordWithRelationArray, recordType).isEmpty());
	}
	@Test
	@Transactional
	void testGetAllRecordTypes() {
		List<RecordType> typesList = recordDao.getAllRecordTypes(instanceId);
		assertEquals(1, typesList.size());
		assertTrue(typesList.contains(recordType));

		RecordType newRecordType = RecordType.valueOf("newRecordType");
		recordDao.createRecordType(instanceId, Collections.emptyMap(), newRecordType, new RelationCollection(Collections.emptySet(), Collections.emptySet()), RECORD_ID);

		List<RecordType> newTypesList = recordDao.getAllRecordTypes(instanceId);
		assertEquals(2, newTypesList.size());
		assertTrue(newTypesList.contains(recordType));
		assertTrue(newTypesList.contains(newRecordType));
	}

	@Test
	@Transactional
	void testGetAllRecordTypesNoJoins() {
		List<RecordType> typesList = recordDao.getAllRecordTypes(instanceId);
		assertEquals(1, typesList.size());
		assertTrue(typesList.contains(recordType));

		RecordType relationArrayType = RecordType.valueOf("relationArrayType");
		Relation arrayRelation = new Relation("relArrAttr", recordType);
		recordDao.createRecordType(instanceId, Map.of( "relArrAttr", DataTypeMapping.ARRAY_OF_RELATION), relationArrayType,
				new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)), RECORD_ID);

		List<RecordType> newTypesList = recordDao.getAllRecordTypes(instanceId);
		assertEquals(2, newTypesList.size());
		assertTrue(newTypesList.contains(recordType));
		assertTrue(newTypesList.contains(relationArrayType));
	}

	@Test
	@Transactional
	void testCountRecords() {
		// ensure we start with a count of 0 records
		assertEquals(0, recordDao.countRecords(instanceId, recordType));
		// insert records and test the count after each insert
		for (int i = 0; i < 10; i++) {
			Record testRecord = new Record("record" + i, recordType, RecordAttributes.empty());
			recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord), new HashMap<>());
			assertEquals(i + 1, recordDao.countRecords(instanceId, recordType),
					"after inserting " + (i + 1) + " records");
		}
	}

	@Test
	@Transactional
	void testRecordExists() {
		assertFalse(recordDao.recordExists(instanceId, recordType, "aRecord"));
		recordDao.batchUpsert(instanceId, recordType,
				Collections.singletonList(new Record("aRecord", recordType, RecordAttributes.empty())),
				new HashMap<>());
		assertTrue(recordDao.recordExists(instanceId, recordType, "aRecord"));
	}

	@Test
	@Transactional
	void testDeleteRecordType() {
		// make sure type already exists
		assertTrue(recordDao.recordTypeExists(instanceId, recordType));
		recordDao.deleteRecordType(instanceId, recordType);
		// make sure type no longer exists
		assertFalse(recordDao.recordTypeExists(instanceId, recordType));
	}

	@Test
	void testDeleteRecordTypeWithRelation() {
		RecordType recordTypeName = recordType;
		RecordType referencedType = RecordType.valueOf("referencedType");
		recordDao.createRecordType(instanceId, Collections.emptyMap(), referencedType, new RelationCollection(Collections.emptySet(), Collections.emptySet()), RECORD_ID);

		recordDao.addColumn(instanceId, recordTypeName, "relation", DataTypeMapping.RELATION, referencedType);

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, referencedType, RecordAttributes.empty());
		recordDao.batchUpsert(instanceId, referencedType, Collections.singletonList(referencedRecord), new HashMap<>());

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(referencedType, refRecordId);
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("relation", reference)));
		recordDao.batchUpsert(instanceId, recordTypeName, Collections.singletonList(testRecord),
				new HashMap<>(Map.of("relation", DataTypeMapping.RELATION)));

		// Should throw an error
		assertThrows(ResponseStatusException.class, () -> {
			recordDao.deleteRecordType(instanceId, referencedType);
		}, "Exception should be thrown when attempting to delete record type with relation");
	}

	@Test
	void testDeleteRecordTypeWithRelationArray() {
		//create records to be referenced
		String refRecordId = "referencedRecord1";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		String refRecordId2 = "referencedRecord2";
		Record referencedRecord2 = new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
		recordDao.batchUpsert(instanceId, recordType, List.of(referencedRecord, referencedRecord2), new HashMap<>());

		//Create referencing record type
		RecordType relationArrayType = RecordType.valueOf("relationArrayType");
		Relation arrayRelation = new Relation("relArrAttr", recordType);
		Map<String, DataTypeMapping> schema = Map.of("relArrAttr", DataTypeMapping.ARRAY_OF_RELATION);
		recordDao.createRecordType(instanceId, schema, relationArrayType,
				new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)), RECORD_ID);

		//Create record with relation array
		String relArrId = "recordWithRelationArr";
		List<String> relArr = List.of(RelationUtils.createRelationString(recordType, refRecordId), RelationUtils.createRelationString(recordType, refRecordId2));
		Record recordWithRelationArray = new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
		recordDao.batchUpsert(instanceId, relationArrayType, Collections.singletonList(recordWithRelationArray), schema);

		//Normally insert into join is called from the service level, so when working directly with the dao must call it manually
		recordDao.insertIntoJoin(instanceId, arrayRelation, relationArrayType, List.of(new RelationValue(recordWithRelationArray, referencedRecord), new RelationValue(recordWithRelationArray, referencedRecord2)));

		//Delete record type
		recordDao.deleteRecordType(instanceId, relationArrayType);

		//Record table should have been deleted
		assertFalse(recordDao.recordTypeExists(instanceId, relationArrayType));

		//check that join table is gone as well
		assertFalse(testDao.joinTableExists(instanceId, "relArrAttr", relationArrayType ));
	}

	@Test
	@Transactional
	void testCreateRelationJoinTable(){
		RecordType secondRecordType = RecordType.valueOf("secondRecordType");
		recordDao.createRecordType(instanceId, Collections.emptyMap(), secondRecordType, new RelationCollection(Collections.emptySet(), Collections.emptySet()), RECORD_ID);

		recordDao.createRelationJoinTable(instanceId, "refArray",
				secondRecordType, recordType);

		List<Relation> relationArrays = recordDao.getRelationArrayCols(instanceId, secondRecordType);
		assertEquals(1, relationArrays.size());
		assertTrue(relationArrays.contains(new Relation("refArray", recordType)));
		assertTrue(testDao.joinTableExists(instanceId, "refArray", secondRecordType));
	}

	@Test
	@Transactional
	void testCreateRecordTypeWithRelationArray() {
		RecordType relationarrayType = RecordType.valueOf("relationArrayType");
		Relation singleRelation = new Relation("refAttr", recordType);
		Relation arrayRelation = new Relation("relArrAttr", recordType);
		recordDao.createRecordType(instanceId, Map.of("stringAttr", DataTypeMapping.STRING, "refAttr", DataTypeMapping.RELATION, "relArrAttr", DataTypeMapping.ARRAY_OF_RELATION), relationarrayType,
		new RelationCollection(Set.of(singleRelation), Set.of(arrayRelation)), RECORD_ID);

		Map<String, DataTypeMapping> schema = recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, relationarrayType);
		assertEquals(3, schema.size());
		assertEquals(DataTypeMapping.STRING, schema.get("stringAttr"));
		assertEquals(DataTypeMapping.RELATION, schema.get("refAttr"));
		assertEquals(DataTypeMapping.ARRAY_OF_RELATION, schema.get("relArrAttr"));
		List<Relation> relationCols = recordDao.getRelationCols(instanceId, relationarrayType);
		assertEquals(List.of(singleRelation), relationCols);
		List<Relation> relationArrayCols = recordDao.getRelationArrayCols(instanceId, relationarrayType);
		assertEquals(List.of(arrayRelation), relationArrayCols);
		assertTrue(testDao.joinTableExists(instanceId, "relArrAttr", relationarrayType));
	}

	@Test
	@Transactional
	void testCreateAndGetRecordWithRelationArray() {
		// add some records to be relations
		String refRecordId = "referencedRecord1";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		String refRecordId2 = "referencedRecord12";
		Record referencedRecord2 = new Record(refRecordId2, recordType, new RecordAttributes(Map.of("foo", "bar2")));
		recordDao.batchUpsert(instanceId, recordType, List.of(referencedRecord, referencedRecord2), new HashMap<>());

		//Create record type
		RecordType relationArrayType = RecordType.valueOf("relationArrayType");
		Relation arrayRelation = new Relation("relArrAttr", recordType);
		Map<String, DataTypeMapping> schema = Map.of("stringAttr", DataTypeMapping.STRING, "refAttr", DataTypeMapping.RELATION, "relArrAttr", DataTypeMapping.ARRAY_OF_RELATION);
		recordDao.createRecordType(instanceId, schema, relationArrayType,
		new RelationCollection(Collections.emptySet(), Set.of(arrayRelation)), RECORD_ID);

		//Create record with relation array
		String relArrId = "recordWithRelationArr";
		List<String> relArr = List.of(RelationUtils.createRelationString(recordType, refRecordId), RelationUtils.createRelationString(recordType, refRecordId2));
		Record recordWithRelationArray = new Record(relArrId, relationArrayType, new RecordAttributes(Map.of("relArrAttr", relArr)));
		recordDao.batchUpsert(instanceId, relationArrayType, Collections.singletonList(recordWithRelationArray), schema);

		Map<String, DataTypeMapping> createdSchema = recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, relationArrayType);
		assertEquals(3, createdSchema.size());
		List<Relation> relationArrayCols = recordDao.getRelationArrayCols(instanceId, relationArrayType);
		assertEquals(List.of(arrayRelation), relationArrayCols);
		Record record = recordDao.getSingleRecord(instanceId, relationArrayType, relArrId).get();
		assertNotNull(record);
		String[] actualAttrValue = assertInstanceOf(String[].class, record.getAttributeValue("relArrAttr"));
		assertIterableEquals(relArr, Arrays.asList(actualAttrValue));

		//The purpose of inserting in to the join is to make sure foreign keys are consistent
		//So we need to make sure no error is thrown
		assertDoesNotThrow(() -> recordDao.insertIntoJoin(instanceId, arrayRelation, relationArrayType, List.of(new RelationValue(record, referencedRecord), new RelationValue(record, referencedRecord2))));
		assertEquals(List.of(refRecordId, refRecordId2), testDao.getRelationArrayValues(instanceId, "relArrAttr", record, recordType));
	}
	@Test
	@Transactional
	void testGetRelationArrayColumns(){
		//Add relation array columns to a type
		RecordType relationarrayType = RecordType.valueOf("relationArrayType");
		Relation arrayRelation1 = new Relation("relArr1", recordType);
		Relation arrayRelation2 = new Relation("relArr2", recordType);
		recordDao.createRecordType(instanceId, Map.of( "relArr1", DataTypeMapping.ARRAY_OF_RELATION, "relArr2", DataTypeMapping.ARRAY_OF_RELATION), relationarrayType,
				new RelationCollection(Collections.emptySet(), Set.of(arrayRelation1, arrayRelation2)), RECORD_ID);

		List<Relation> cols = recordDao.getRelationArrayCols(instanceId, relationarrayType);
		assertEquals(2, cols.size());
		assertTrue(cols.contains(arrayRelation1));
		assertTrue(cols.contains(arrayRelation2));
	}

	@Test
	@Transactional
	void testRemoveFromJoin() {
		//create records to reference in join table
		String fromRecordId = "fromRecord1";
		Record fromRecord = new Record(fromRecordId, recordType, new RecordAttributes(new HashMap<>()));
		String fromRecordId2 = "fromRecord2";
		Record fromRecord2 = new Record(fromRecordId2, recordType, new RecordAttributes(new HashMap<>()));
		String fromRecordId3 = "fromRecord3";
		Record fromRecord3 = new Record(fromRecordId3, recordType, new RecordAttributes(new HashMap<>()));
		recordDao.batchUpsert(instanceId, recordType, List.of(fromRecord, fromRecord2, fromRecord3), new HashMap<>());

		RecordType toType = RecordType.valueOf("toType");
		recordDao.createRecordType(instanceId, new HashMap<>(), toType,
				new RelationCollection(Collections.emptySet(), Collections.emptySet()), RECORD_ID);
		String toRecordId = "toRecord1";
		Record toRecord = new Record(toRecordId, toType, new RecordAttributes(new HashMap<>()));
		String toRecordId2 = "toRecord2";
		Record toRecord2 = new Record(toRecordId2, toType, new RecordAttributes(new HashMap<>()));
		recordDao.batchUpsert(instanceId, toType, List.of(toRecord, toRecord2), new HashMap<>());

		//create join table
		recordDao.createRelationJoinTable(instanceId, "referenceArray", recordType, toType);

		//insert into join table
		Relation rel = new Relation("referenceArray", toType);
		recordDao.insertIntoJoin(instanceId, rel, recordType, List.of(
				new RelationValue(fromRecord, toRecord),new RelationValue(fromRecord, toRecord2),
				new RelationValue(fromRecord2, toRecord),new RelationValue(fromRecord2, toRecord2),
				new RelationValue(fromRecord3, toRecord),new RelationValue(fromRecord3, toRecord2)));

		//Check that values are in join table
		List<String> joinVals1 = testDao.getRelationArrayValues(instanceId, "referenceArray", fromRecord, toType);
		assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals1);
		List<String> joinVals2 = testDao.getRelationArrayValues(instanceId, "referenceArray", fromRecord2, toType);
		assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals2);
		List<String> joinVals3 = testDao.getRelationArrayValues(instanceId, "referenceArray", fromRecord3, toType);
		assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals3);

		//remove from join table
		recordDao.removeFromJoin(instanceId, rel, recordType, List.of(fromRecordId, fromRecordId3));

		//Make sure values have been removed
		joinVals1 = testDao.getRelationArrayValues(instanceId, "referenceArray", fromRecord, toType);
		assert(joinVals1.isEmpty());
		joinVals3 = testDao.getRelationArrayValues(instanceId, "referenceArray", fromRecord3, toType);
		assert(joinVals3.isEmpty());
		//But not other values
		joinVals2 = testDao.getRelationArrayValues(instanceId, "referenceArray", fromRecord2, toType);
		assertIterableEquals(List.of(toRecordId, toRecordId2), joinVals2);
	}

}
