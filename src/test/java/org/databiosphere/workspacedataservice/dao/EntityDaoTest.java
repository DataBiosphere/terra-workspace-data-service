package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityDaoTest {

    @Autowired
    EntityDao entityDao;
    UUID workspaceId;
    EntityType entityType;
    Long entityTypeId;

    @BeforeAll
    void setUp(){
        workspaceId = entityDao.getWorkspaceId("default", "test");
        entityType  = new EntityType("testEntityType");
        entityTypeId = entityDao.loadEntityType(entityType, workspaceId);
    }

    @Test
    @Transactional
    void testGetSingleEntity(){
        //add entity
        EntityId entityId = new EntityId("testEntity");
        Entity testEntity = new Entity(entityId, entityType, new HashMap<>(), entityTypeId);
        entityDao.batchUpsert(List.of(testEntity));

        //make sure entity is fetched
        Entity search = entityDao.getSingleEntity(workspaceId, entityType, entityId);
        assertEquals(testEntity, search);

        //nonexistent entity should be null
        Entity none = entityDao.getSingleEntity(workspaceId, entityType, new EntityId("noEntity"));
        assertNull(none);
    }
}
