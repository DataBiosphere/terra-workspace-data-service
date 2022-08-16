package org.databiosphere.workspacedataservice.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.InvalidRelation;
import org.databiosphere.workspacedataservice.service.model.MissingReferencedTableException;
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

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RecordDaoTest {

  @Autowired
  RecordDao entityDao;
  UUID workspaceId;
  RecordType recordType;

  @BeforeEach
  void setUp() throws MissingReferencedTableException {
    workspaceId = UUID.randomUUID();
    recordType = new RecordType("testEntityType");
    entityDao.createSchema(workspaceId);
    entityDao.createReccordType(
        workspaceId, Collections.emptyMap(), recordType.getName(), Collections.emptySet());
  }

  @Test
  @Transactional
  void testGetSingleEntity() throws InvalidRelation {
    // add entity
    RecordId recordId = new RecordId("testEntity");
    Record testRecord = new Record(recordId, recordType, new RecordAttributes(new HashMap<>()));
    entityDao.batchUpsert(
        workspaceId,
        recordType.getName(),
        Collections.singletonList(testRecord),
        new LinkedHashMap<>());

    // make sure entity is fetched
    Record search =
        entityDao.getSingleRecord(workspaceId, recordType, recordId, Collections.emptyList());
    assertEquals(testRecord, search);

    // nonexistent entity should be null
    Record none =
        entityDao.getSingleRecord(
            workspaceId, recordType, new RecordId("noEntity"), Collections.emptyList());
    assertNull(none);
  }

  @Test
  @Transactional
  void testCreateSingleEntity() throws InvalidRelation {
    entityDao.addColumn(workspaceId, recordType.getName(), "foo", DataTypeMapping.STRING);

    // create entity with no attributes
    RecordId recordId = new RecordId("testEntity");
    Record testRecord = new Record(recordId, recordType, new RecordAttributes(new HashMap<>()));
    entityDao.batchUpsert(
        workspaceId,
        recordType.getName(),
        Collections.singletonList(testRecord),
        new LinkedHashMap<>());

    Record search =
        entityDao.getSingleRecord(workspaceId, recordType, recordId, Collections.emptyList());
    assertEquals(testRecord, search, "Created entity should match entered entity");

    // create entity with attributes
    RecordId attrId = new RecordId("entityWithAttr");
    Record recordWithAttr =
        new Record(attrId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    entityDao.batchUpsert(
        workspaceId,
        recordType.getName(),
        Collections.singletonList(recordWithAttr),
        new LinkedHashMap<>(Map.of("foo", DataTypeMapping.STRING)));

    search = entityDao.getSingleRecord(workspaceId, recordType, attrId, Collections.emptyList());
    assertEquals(
            recordWithAttr, search, "Created entity with attributes should match entered entity");
  }

  @Test
  @Transactional
  void testCreateEntityWithReferences()
      throws MissingReferencedTableException, InvalidRelation {
    // make sure columns are in entitytype, as this will be taken care of before we get to the dao
    entityDao.addColumn(workspaceId, recordType.getName(), "foo", DataTypeMapping.STRING);

    entityDao.addColumn(
        workspaceId, recordType.getName(), "testEntityType", DataTypeMapping.STRING);
    entityDao.addForeignKeyForReference(
        recordType.getName(), recordType.getName(), workspaceId, "testEntityType");

    RecordId refRecordId = new RecordId("referencedEntity");
    Record referencedRecord =
        new Record(refRecordId, recordType, new RecordAttributes(Map.of("foo", "bar")));
    entityDao.batchUpsert(
        workspaceId,
        recordType.getName(),
        Collections.singletonList(referencedRecord),
        new LinkedHashMap<>());

    RecordId recordId = new RecordId("testEntity");
    String reference = RelationUtils.createRelationString("testEntityType", "referencedEntity");
    Record testRecord =
        new Record(recordId, recordType, new RecordAttributes(Map.of("testEntityType", reference)));
    entityDao.batchUpsert(
        workspaceId,
        recordType.getName(),
        Collections.singletonList(testRecord),
        new LinkedHashMap<>(
            Map.of("foo", DataTypeMapping.STRING, "testEntityType", DataTypeMapping.STRING)));

    Record search =
        entityDao.getSingleRecord(
            workspaceId,
                recordType,
                recordId,
            entityDao.getReferenceCols(workspaceId, recordType.getName()));
    assertEquals(testRecord, search, "Created entity with references should match entered entity");
  }

  @Test
  @Transactional
  void testGetReferenceCols() throws MissingReferencedTableException {
    entityDao.addColumn(workspaceId, recordType.getName(), "referenceCol", DataTypeMapping.STRING);
    entityDao.addForeignKeyForReference(
        recordType.getName(), recordType.getName(), workspaceId, "referenceCol");

    List<Relation> refCols = entityDao.getReferenceCols(workspaceId, recordType.getName());
    assertEquals(1, refCols.size(), "There should be one referenced column");
    assertEquals(
        "referenceCol",
        refCols.get(0).referenceColName(),
        "Reference column should be named referenceCol");
  }
}
