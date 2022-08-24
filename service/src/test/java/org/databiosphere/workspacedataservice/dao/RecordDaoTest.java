package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordDaoTest {

	@Autowired
	RecordDao recordDao;
	UUID instanceId;
	RecordType recordType;

	@BeforeEach
	void setUp() {
		instanceId = UUID.randomUUID();
		recordType = new RecordType("testRecordType");
		recordDao.createSchema(instanceId);
		recordDao.createRecordType(instanceId, Collections.emptyMap(), recordType.getName(), Collections.emptySet());
	}

	@Test
	@Transactional
	void testGetSingleRecord() {
		// add record
		RecordId recordId = new RecordId("testRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(new HashMap<>()));
		recordDao.batchUpsert(instanceId, recordType.getName(), Collections.singletonList(testRecord),
				new HashMap<>());

		// make sure record is fetched
		Record search = recordDao.getSingleRecord(instanceId, recordType, recordId, Collections.emptyList()).get();
		assertEquals(testRecord, search);

		// nonexistent record should be null
		Optional<Record> none = recordDao.getSingleRecord(instanceId, recordType, new RecordId("noRecord"),
				Collections.emptyList());
		assertEquals(none, Optional.empty());
	}

	@Test
	@Transactional
	void testCreateSingleRecord() {
		recordDao.addColumn(instanceId, recordType.getName(), "foo", DataTypeMapping.STRING);

		// create record with no attributes
		RecordId recordId = new RecordId("testRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(new HashMap<>()));
		recordDao.batchUpsert(instanceId, recordType.getName(), Collections.singletonList(testRecord),
				new HashMap<>());

		Record search = recordDao.getSingleRecord(instanceId, recordType, recordId, Collections.emptyList()).get();
		assertEquals(testRecord, search, "Created record should match entered record");

		// create record with attributes
		RecordId attrId = new RecordId("recordWithAttr");
		Record recordWithAttr = new Record(attrId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType.getName(), Collections.singletonList(recordWithAttr),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING)));

		search = recordDao.getSingleRecord(instanceId, recordType, attrId, Collections.emptyList()).get();
		assertEquals(recordWithAttr, search, "Created record with attributes should match entered record");
	}

	@Test
	@Transactional
	void testCreateRecordWithRelations() {
		// make sure columns are in recordType, as this will be taken care of before we
		// get to the dao
		recordDao.addColumn(instanceId, recordType.getName(), "foo", DataTypeMapping.STRING);

		recordDao.addColumn(instanceId, recordType.getName(), "testRecordType", DataTypeMapping.STRING);
		recordDao.addForeignKeyForReference(recordType.getName(), recordType.getName(), instanceId, "testRecordType");

		RecordId refRecordId = new RecordId("referencedRecord");
		Record referencedRecord = new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
		recordDao.batchUpsert(instanceId, recordType.getName(), Collections.singletonList(referencedRecord),
				new HashMap<>());

		RecordId recordId = new RecordId("testRecord");
		String reference = RelationUtils.createRelationString("testRecordType", "referencedRecord");
		Record testRecord = new Record(recordId, recordType, new RecordAttributes(Map.of("testRecordType", reference)));
		recordDao.batchUpsert(instanceId, recordType.getName(), Collections.singletonList(testRecord),
				new HashMap<>(Map.of("foo", DataTypeMapping.STRING, "testRecordType", DataTypeMapping.STRING)));

		Record search = recordDao.getSingleRecord(instanceId, recordType, recordId,
				recordDao.getRelationCols(instanceId, recordType.getName())).get();
		assertEquals(testRecord, search, "Created record with references should match entered record");
	}

	@Test
	@Transactional
	void testGetReferenceCols() {
		recordDao.addColumn(instanceId, recordType.getName(), "referenceCol", DataTypeMapping.STRING);
		recordDao.addForeignKeyForReference(recordType.getName(), recordType.getName(), instanceId, "referenceCol");

		List<Relation> refCols = recordDao.getRelationCols(instanceId, recordType.getName());
		assertEquals(1, refCols.size(), "There should be one referenced column");
		assertEquals("referenceCol", refCols.get(0).relationColName(), "Reference column should be named referenceCol");
	}
}
