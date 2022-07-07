package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.model.EntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@SpringBootTest
class EntityReferenceServiceTest {

    @MockBean
    EntityDao entityDao;

    private EntityReferenceService entityReferenceService;
    private UUID testWorkspaceUUID = UUID.randomUUID();
    private Long entityTypeId = 123456789L;
    private EntityType entityType = new EntityType("testEntityType");

    @BeforeEach
    void setup(){
        entityReferenceService = new EntityReferenceService(entityDao);
        when(entityDao.getEntityTypeId(testWorkspaceUUID, "testEntityType")).thenReturn(entityTypeId);
    }

    @Test
    void testGetEntityReferences() {
        Map<String, Object> simpleAttributes = new HashMap<>();
        simpleAttributes.put("attr1", "attr1value");
        Entity testEntity = new Entity("testEntity", entityType, simpleAttributes, entityTypeId);

        Map<String, Object> refAttr = new HashMap<>();
        refAttr.put("entityType", "entityType");
        refAttr.put("entityName", "testEntity");
        Map<String, Object> referencingAttributes = new HashMap<>();
        referencingAttributes.put("ref", refAttr);
        Entity referencingEntity = new Entity("referencingEntity", entityType, referencingAttributes, entityTypeId);

        List<Entity> entities = new ArrayList<>();
        List<EntityReference> result = entityReferenceService.getEntityReferences(entities, testWorkspaceUUID);

        assertTrue(result.isEmpty(), "Empty entity list should return an empty list of entity references.");

        entities.add(testEntity);
        result = entityReferenceService.getEntityReferences(entities, testWorkspaceUUID);

        assertTrue(result.isEmpty(), "Entities that do not refer to other entities should result in an empty list of entity references.");

        entities.add(referencingEntity);
        result = entityReferenceService.getEntityReferences(entities, testWorkspaceUUID);

        assertFalse(result.isEmpty(), "Entities referencing other entities should result in an EntityReference.");
    }

}