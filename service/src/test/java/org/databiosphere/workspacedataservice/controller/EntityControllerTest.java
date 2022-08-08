package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityControllerTest {

    @MockBean
    EntityDao entityDao;

    EntityType testEntityType = new EntityType("test-type");
    EntityId testEntityId = new EntityId("test");
    //Class under test
    EntityController controller;

    @BeforeAll
    void setUp(){
        controller = new EntityController(entityDao);
    }

    @Test
    void testPatchNewSingleEntity(){
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(new Entity(testEntityId,
                testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01"))))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))));
        ResponseEntity<EntityResponse> response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, new EntityAttributes(Map.of("foo", "bar"))));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar"), response.getBody().entityAttributes().getAttributes());
    }
    @Test
    void testPatchSingleEntityOverwriteAttr(){
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(new Entity(testEntityId,
                testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "baz"))));
        ResponseEntity<EntityResponse> response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, new EntityAttributes(Map.of("foo", "baz"))));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "baz"), response.getBody().entityAttributes().getAttributes());
    }
    @Test
    void testPatchNewSingleEntityWithReference(){
        when(entityDao.getReferenceCols(any(), any())).thenReturn(Collections.singletonList(new SingleTenantEntityReference("referencingAttr", testEntityType)));
        Map<String, Object> refAttr = new HashMap<>();
        refAttr.put("entityType", testEntityType.getName());
        refAttr.put("entityName", testEntityId.getEntityIdentifier());
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar", "referencingAttr", refAttr))));
        EntityAttributes referencingAttributes = new EntityAttributes(Map.of("referencingAttr", refAttr));
        ResponseEntity<EntityResponse> response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, new EntityId("referencingEntity"),
                new EntityRequest(testEntityId, testEntityType, referencingAttributes));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 3);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar", "referencingAttr", refAttr), response.getBody().entityAttributes().getAttributes());
    }

    @Test
    void testGetSingleEntity(){
        Entity testEntity = new Entity(testEntityId,
                testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar")));
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(testEntity);
        ResponseEntity<EntityResponse> response = controller.getSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId);
        assertEquals(testEntity.getAttributes(), response.getBody().entityAttributes());
        assertEquals(testEntity.getName(), response.getBody().entityId());
        assertEquals(testEntity.getEntityType(), response.getBody().entityType());
    }

    @Test
    void testGetNonexistentSingleEntity(){
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> {
            controller.getSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId);
        });

        assertTrue(exception.getMessage().contains("Entity not found"));
        assertEquals(exception.getStatus(), HttpStatus.NOT_FOUND);
    }
}
