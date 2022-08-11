package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.InvalidEntityReference;
import org.databiosphere.workspacedataservice.service.model.MissingReferencedTableException;
import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityDaoTest {

    @Autowired
    EntityDao entityDao;
    UUID workspaceId;
    EntityType entityType;

    @BeforeEach
    void setUp() throws MissingReferencedTableException {
        workspaceId = UUID.randomUUID();
        entityType  = new EntityType("testEntityType");
        entityDao.createSchema(workspaceId);
        entityDao.createEntityType(workspaceId, Collections.emptyMap(), entityType.getName(), Collections.emptySet());
    }

    @Test
    @Transactional
    void testGetSingleEntity(){
        //add entity
        EntityId entityId = new EntityId("testEntity");
        Entity testEntity = new Entity(entityId, entityType, new EntityAttributes(new HashMap<>()));
        entityDao.batchUpsert(workspaceId, entityType.getName(), Collections.singletonList(testEntity), new LinkedHashMap<>());

        //make sure entity is fetched
        Entity search = entityDao.getSingleEntity(workspaceId, entityType, entityId, Collections.emptyList());
        assertEquals(testEntity, search);

        //nonexistent entity should be null
        Entity none = entityDao.getSingleEntity(workspaceId, entityType, new EntityId("noEntity"), Collections.emptyList());
        assertNull(none);
    }

    @Test
    @Transactional
    void testCreateSingleEntity() throws InvalidEntityReference {
        entityDao.addColumn(workspaceId, entityType.getName(), "foo", DataTypeMapping.STRING);

        //create entity with no attributes
        EntityId entityId = new EntityId("testEntity");
        Entity testEntity = new Entity(entityId, entityType, new EntityAttributes(new HashMap<>()));
        entityDao.createSingleEntity(workspaceId, entityType.getName(), testEntity, new LinkedHashMap<>());

        Entity search = entityDao.getSingleEntity(workspaceId, entityType, entityId, Collections.emptyList());
        assertEquals(testEntity, search, "Created entity should match entered entity");

        //create entity with attributes
        EntityId attrId = new EntityId("entityWithAttr");
        Entity entityWithAttr = new Entity(attrId, entityType, new EntityAttributes(Map.of("foo", "bar")));
        entityDao.createSingleEntity(workspaceId, entityType.getName(), entityWithAttr, new LinkedHashMap<>(Map.of("foo", DataTypeMapping.STRING)));

        search = entityDao.getSingleEntity(workspaceId, entityType, attrId, Collections.emptyList());
        assertEquals(entityWithAttr, search, "Created entity with attributes should match entered entity");
    }

    @Test
    @Transactional
    void testCreateEntityWithReferences() throws MissingReferencedTableException, InvalidEntityReference {
        //make sure columns are in entitytype, as this will be taken care of before we get to the dao
        entityDao.addColumn(workspaceId, entityType.getName(), "foo", DataTypeMapping.STRING);

        entityDao.addColumn(workspaceId, entityType.getName(), "testEntityType", DataTypeMapping.STRING);
        entityDao.addForeignKeyForReference(entityType.getName(), entityType.getName(), workspaceId, "testEntityType");

        EntityId refEntityId = new EntityId("referencedEntity");
        Entity referencedEntity = new Entity(refEntityId, entityType, new EntityAttributes(Map.of("foo", "bar")));
        //avoid using method under test to prepare db - may want to change to creating this entity during setup
        entityDao.batchUpsert(workspaceId, entityType.getName(), Collections.singletonList(referencedEntity), new LinkedHashMap<>());

        EntityId entityId = new EntityId("testEntity");
        Map<String,String> reference = Map.of(SingleTenantEntityReference.ENTITY_TYPE_KEY, "testEntityType", SingleTenantEntityReference.ENTITY_NAME_KEY, "referencedEntity");
        Entity testEntity = new Entity(entityId,entityType, new EntityAttributes(Map.of("testEntityType", reference)));
        entityDao.createSingleEntity(workspaceId, entityType.getName(), testEntity,
                new LinkedHashMap<>(Map.of("foo", DataTypeMapping.STRING, "testEntityType", DataTypeMapping.STRING)));

        Entity search = entityDao.getSingleEntity(workspaceId, entityType, entityId, entityDao.getReferenceCols(workspaceId, entityType.getName()));
        assertEquals(testEntity, search, "Created entity with references should match entered entity");
    }

    @Test
    @Transactional
    void testGetReferenceCols() throws MissingReferencedTableException{
        entityDao.addColumn(workspaceId, entityType.getName(), "referenceCol", DataTypeMapping.STRING);
        entityDao.addForeignKeyForReference(entityType.getName(), entityType.getName(), workspaceId, "referenceCol");

        List<SingleTenantEntityReference> refCols = entityDao.getReferenceCols(workspaceId, entityType.getName());
        assertEquals(1, refCols.size(), "There should be one referenced column");
        assertEquals("referenceCol", refCols.get(0).getReferenceColName(), "Reference column should be named referenceCol");
    }
}
