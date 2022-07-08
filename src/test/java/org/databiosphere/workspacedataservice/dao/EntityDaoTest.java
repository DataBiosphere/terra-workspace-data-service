package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityDaoTest {

    @Autowired
    EntityDao entityDao;
    UUID workspaceId;
    EntityType entityType = new EntityType("testEntityType");

    @BeforeAll
    void setUp(){
        workspaceId = entityDao.getWorkspaceId("default", "test");
    }

    @Test
    void testGetSingleEntity(){
        Long entityTypeId = entityDao.loadEntityType(entityType, workspaceId);
        Entity testEntity = new Entity("testEntity", entityType, new HashMap<>(), entityTypeId);
        entityDao.batchUpsert(List.of(testEntity));
        Entity search = entityDao.getSingleEntity(workspaceId, entityType, new EntityId("testEntity"));
        assertEquals(testEntity, search);
    }
}
