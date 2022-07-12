package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.EntityReferenceService;
import org.databiosphere.workspacedataservice.service.model.EntityReference;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
public class PatchSingleEntityTest {

    @MockBean
    EntityDao entityDao;


    @Test
    void testPatchSingleEntity(){
        //New entity
        EntityType testEntityType = new EntityType("test-type");
        EntityId testEntityId = new EntityId("test");
        when(entityDao.getSingleEntity(any(), any(), any())).thenReturn(new Entity(testEntityId,
                testEntityType, new HashMap<>(Map.of("created_at", "2022-10-01")), 1L));
        EntityReferenceService referenceService = new EntityReferenceService(entityDao);
        EntityController controller = new EntityController(referenceService, entityDao);
        ResponseEntity<EntityResponse> response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, new EntityAttributes(Map.of("foo", "bar"))));
        assertTrue(response.getBody().entityAttributes().attributes().size() == 2);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar"), response.getBody().entityAttributes().attributes());

        //Overwriting attribute
        when(entityDao.getSingleEntity(any(), any(), any())).thenReturn(new Entity(testEntityId,
                testEntityType, new HashMap<>(Map.of("created_at", "2022-10-01", "foo", "bar")), 1L));
        response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, new EntityAttributes(Map.of("foo", "baz"))));
        assertTrue(response.getBody().entityAttributes().attributes().size() == 2);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "baz"), response.getBody().entityAttributes().attributes());

        //Add a reference
        Map<String, Object> refAttr = new HashMap<>();
        refAttr.put("entityType", testEntityType);
        refAttr.put("entityName", testEntityId);
        EntityAttributes referencingAttributes = new EntityAttributes(Map.of("referencingAttr", refAttr));
        response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, new EntityId("referencingEntity"),
                new EntityRequest(testEntityId, testEntityType, referencingAttributes));
        System.out.println(response.getBody().entityAttributes());
        assertTrue(response.getBody().entityAttributes().attributes().size() == 3);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "baz", "referencingAttr", refAttr), response.getBody().entityAttributes().attributes());

    }
}
