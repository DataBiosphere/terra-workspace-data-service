package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.model.MissingReferencedTableException;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityDaoTest {

    @Autowired
    EntityDao entityDao;
    UUID workspaceId;
    EntityType entityType;

    @BeforeAll
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
}
