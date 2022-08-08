package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.MissingReferencedTableException;
import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

import static org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference.ENTITY_NAME_KEY;
import static org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference.ENTITY_TYPE_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

    //TODO: this test seems invalid, since updateSingleEntity just returns the result of getSingleEntity, which is mocked
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
    void testPutNewSingleEntity(){
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(null)
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))));
        when(entityDao.entityTypeExists(any(), eq(testEntityType.getName()))).thenReturn(true);
        ResponseEntity<EntityResponse> response = controller.putSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar"), response.getBody().entityAttributes().getAttributes());
    }

    @Test
    void testPutNewSingleEntityWithNewEntityType() throws MissingReferencedTableException {
        EntityType newEntityType = new EntityType("newEntityType");
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(null)
                .thenReturn(new Entity(testEntityId,
                        newEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))));
        when(entityDao.entityTypeExists(any(), eq(newEntityType.getName()))).thenReturn(false);
        UUID workspaceId = UUID.randomUUID();

        //make sure createEntityType is called
        doAnswer(invocation -> {
            Object instanceId = invocation.getArgument(0);
            Object schema = invocation.getArgument(1);
            Object entityTypeName = invocation.getArgument(2);
            Object refs = invocation.getArgument(3);

            assertEquals(workspaceId, instanceId, "createEntityType should be called with workspaceId %s".formatted(workspaceId));
            assertEquals(Map.of("created_at", DataTypeMapping.DATE, "foo", DataTypeMapping.STRING), schema, "createEntityType should be called with schema %s".formatted(Map.of("created_at", DataTypeMapping.DATE, "foo", DataTypeMapping.STRING)));
            assertEquals(newEntityType.getName(), entityTypeName, "createEntityType should be called with entityTypeName %s".formatted(newEntityType.getName()));
            //TODO: should it though?  what should be put here?
            assertEquals(new HashSet<>(), refs, "createEntityType should be called with an empty HashSet");
            return null;
        }).when(entityDao).createEntityType(any(), any(), any(), any());
        ResponseEntity<EntityResponse> response = controller.putSingleEntity(workspaceId, "v0.2", newEntityType, testEntityId,
                new EntityRequest(testEntityId, newEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))));

        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2, "EntityAttributes should contain two items");
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar"), response.getBody().entityAttributes().getAttributes(), "EntityAttributes should contain created_at and foo");
        assertEquals(newEntityType, response.getBody().entityType(), "EntityType should match newEntityType");
    }
    @Test
    void testPutSingleEntityOverwriteAttr(){
        EntityAttributes replacementAttr = new EntityAttributes(Map.of("created_at", "2021-10-01", "bar", "bat"));
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"))))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, replacementAttr));
        UUID workspaceId = UUID.randomUUID();
        //make sure replaceAttributes is called
        doAnswer(invocation -> {
            Object entityName = invocation.getArgument(0);
            Object attributes = invocation.getArgument(1);
            Object instanceId = invocation.getArgument(2);

            assertEquals(workspaceId, instanceId, "replaceAttributes should be called with workspaceId %s".formatted(workspaceId));
            assertEquals(replacementAttr, attributes, "replaceAttributes should be called with attributes %s".formatted(replacementAttr));
            assertEquals(testEntityId.getEntityIdentifier(), entityName, "replaceAttributes should be called with entityTypeName %s".formatted(testEntityId.getEntityIdentifier()));
            return null;
        }).when(entityDao).replaceAttributes(any(), any(), any());

        ResponseEntity<EntityResponse> response = controller.putSingleEntity(workspaceId, "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, replacementAttr));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(replacementAttr, response.getBody().entityAttributes());
    }
    @Test
    void testPutNewSingleEntityWithReference(){
        when(entityDao.getReferenceCols(any(), any())).thenReturn(Collections.singletonList(new SingleTenantEntityReference("referencingAttr", testEntityType)));
        Map<String, Object> refAttr = new HashMap<>();
        refAttr.put("entityType", testEntityType.getName());
        refAttr.put("entityName", testEntityId.getEntityIdentifier());
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(null)
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("referencingAttr", refAttr))));
        EntityAttributes referencingAttributes = new EntityAttributes(Map.of("referencingAttr", refAttr));

        EntityType referencingEntityType = new EntityType("referencingEntityType");
        EntityId referencingEntity = new EntityId("referencingEntity");

        ResponseEntity<EntityResponse> response = controller.putSingleEntity(UUID.randomUUID(), "v0.2", referencingEntityType, referencingEntity,
                new EntityRequest(referencingEntity, referencingEntityType, referencingAttributes));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 1, "Entity attributes should have 1 item");
        assertEquals(Map.of("referencingAttr", refAttr), response.getBody().entityAttributes().getAttributes(),
                "Entity attributes should contain referencingAttr");
    }

    @Test
    @Transactional
    void testPutEntityWithNoChanges(){
        EntityAttributes attr = new EntityAttributes(Map.of("created_at", "2021-10-01", "foo", "bar"));
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(new Entity(testEntityId,
                        testEntityType, attr));
        ResponseEntity<EntityResponse> response = controller.putSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, attr));
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(attr, response.getBody().entityAttributes());
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
