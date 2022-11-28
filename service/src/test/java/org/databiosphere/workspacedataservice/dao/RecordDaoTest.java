package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordDaoTest {

	private static final String PRIMARY_KEY = "row_id";
	@Autowired
	RecordDao recordDao;
	UUID instanceId;
	RecordType recordType;

	@BeforeEach
	void setUp() {
		instanceId = UUID.randomUUID();
		recordType = RecordType.valueOf("testRecordType");
		recordDao.createSchema(instanceId);
		recordDao.createRecordType(instanceId, Collections.emptyMap(), recordType, Collections.emptySet(), PRIMARY_KEY);
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
		recordDao.createRecordType(instanceId, Map.of("attr1", DataTypeMapping.STRING), funkyPk, Collections.emptySet(), sample_id);
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
		Map<String, DataTypeMapping> schema = Map.of("attr1", DataTypeMapping.STRING);
		recordDao.createRecordType(instanceId, schema, referencedRt, Collections.emptySet(), sample_id);
		recordDao.batchUpsert(instanceId, referencedRt, Collections.singletonList(testRecord), Collections.emptyMap(), sample_id);
		RecordType referencer = RecordType.valueOf("referencer");
		recordDao.createRecordType(instanceId, schema, referencer,
				Collections.singleton(new Relation("attr1", referencedRt)), sample_id);
		Record referencerRecord = new Record(recordId, referencer,
				RecordAttributes.empty().putAttribute("attr1", RelationUtils.createRelationString(referencedRt, recordId)));
		recordDao.batchUpsert(instanceId, referencer, Collections.singletonList(referencerRecord), schema, sample_id);
		recordDao.batchDelete(instanceId, referencer, Collections.singletonList(referencerRecord));
		recordDao.batchDelete(instanceId, referencedRt, Collections.singletonList(testRecord));
	}

	@Test
	@Transactional
	void testGetRecordsWithRelations() {
		// Create two records of the same type, one with a value for a relation
		// attribute, the other without
		recordDao.addColumn(instanceId, recordType, "foo", DataTypeMapping.STRING);
		recordDao.addColumn(instanceId, recordType, "relationAttr", DataTypeMapping.STRING);
		recordDao.addForeignKeyForReference(recordType, recordType, instanceId, "relationAttr");

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("relationAttr", reference)));

		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(referencedRecord), new HashMap<>());
		recordDao.batchUpsert(instanceId, recordType, List.of(referencedRecord, testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "relationAttr", DataTypeMapping.STRING)));

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

		recordDao.addColumn(instanceId, recordType, "testRecordType", DataTypeMapping.STRING);
		recordDao.addForeignKeyForReference(recordType, recordType, instanceId, "testRecordType");

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(referencedRecord), new HashMap<>());

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "testRecordType", DataTypeMapping.STRING)));

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
	@Transactional
	void testDeleteRelatedRecord() {
		// make sure columns are in recordType, as this will be taken care of before we
		// get to the dao
		recordDao.addColumn(instanceId, recordType, "foo", DataTypeMapping.STRING);

		recordDao.addColumn(instanceId, recordType, "testRecordType", DataTypeMapping.STRING,
				RecordType.valueOf("testRecordType"));

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(referencedRecord), new HashMap<>());

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(RecordType.valueOf("testRecordType"), "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
		recordDao.batchUpsert(instanceId, recordType, Collections.singletonList(testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "testRecordType", DataTypeMapping.STRING)));

		// Should throw an error
		assertThrows(ResponseStatusException.class, () -> {
			recordDao.deleteSingleRecord(instanceId, recordType, "referencedRecord");
		}, "Exception should be thrown when attempting to delete related record");
	}
	@Test
	@Transactional
	void testGetAllRecordTypes() {
		List<RecordType> typesList = recordDao.getAllRecordTypes(instanceId);
		assertEquals(1, typesList.size());
		assertTrue(typesList.contains(recordType));

		RecordType newRecordType = RecordType.valueOf("newRecordType");
		recordDao.createRecordType(instanceId, Collections.emptyMap(), newRecordType, Collections.emptySet(), RECORD_ID);

		List<RecordType> newTypesList = recordDao.getAllRecordTypes(instanceId);
		assertEquals(2, newTypesList.size());
		assertTrue(newTypesList.contains(recordType));
		assertTrue(newTypesList.contains(newRecordType));
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
	@Transactional
	void testDeleteRecordTypeWithRelation() {
		RecordType recordTypeName = recordType;
		RecordType referencedType = RecordType.valueOf("referencedType");
		recordDao.createRecordType(instanceId, Collections.emptyMap(), referencedType, Collections.emptySet(), RECORD_ID);

		recordDao.addColumn(instanceId, recordTypeName, "relation", DataTypeMapping.STRING, referencedType);

		String refRecordId = "referencedRecord";
		Record referencedRecord = new Record(refRecordId, referencedType, RecordAttributes.empty());
		recordDao.batchUpsert(instanceId, referencedType, Collections.singletonList(referencedRecord), new HashMap<>());

		String recordId = "testRecord";
		String reference = RelationUtils.createRelationString(referencedType, refRecordId);
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("relation", reference)));
		recordDao.batchUpsert(instanceId, recordTypeName, Collections.singletonList(testRecord),
				new HashMap<>(Map.of("relation", DataTypeMapping.STRING)));

		// Should throw an error
		assertThrows(ResponseStatusException.class, () -> {
			recordDao.deleteRecordType(instanceId, referencedType);
		}, "Exception should be thrown when attempting to delete record type with relation");
	}
}
