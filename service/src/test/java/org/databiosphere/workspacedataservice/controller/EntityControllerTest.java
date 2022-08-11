package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.InvalidEntityReference;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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
        when(entityDao.workspaceSchemaExists(any())).thenReturn(true);
        when(entityDao.entityTypeExists(any(), any())).thenReturn(true);
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
        //TODO: This is just checking the mocking.  Is there a better way to test the controller without relying on the dao?
        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar"), response.getBody().entityAttributes().getAttributes());
    }
    @Test
    void testPutNewSingleEntityWithNewEntityType() {
        EntityType newEntityType = new EntityType("newEntityType");
        EntityAttributes newAttributes = new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar"));
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(null)
                .thenReturn(new Entity(testEntityId,
                        newEntityType, newAttributes));
        when(entityDao.entityTypeExists(any(), eq(newEntityType.getName()))).thenReturn(false);
        UUID workspaceId = UUID.randomUUID();

        ResponseEntity<EntityResponse> response = controller.putSingleEntity(workspaceId, "v0.2", newEntityType, testEntityId,
                new EntityRequest(testEntityId, newEntityType, newAttributes));

        //make sure createEntityType is called
//        verify(entityDao).createEntityType(workspaceId, Map.of("created_at", DataTypeMapping.DATE, "foo", DataTypeMapping.STRING), newEntityType.getName(), new HashSet<>());
//        verify(entityDao).createSingleEntity(workspaceId, newEntityType.getName(), new Entity(testEntityId, newEntityType, newAttributes), new LinkedHashMap<>(Map.of("created_at", DataTypeMapping.DATE, "foo", DataTypeMapping.STRING)));

        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2, "EntityAttributes should contain two items");
        assertEquals(Map.of("created_at", "2022-10-01", "foo", "bar"), response.getBody().entityAttributes().getAttributes(), "EntityAttributes should contain created_at and foo");
        assertEquals(newEntityType, response.getBody().entityType(), "EntityType should match newEntityType");
    }
    @Test
    void testPutSingleEntityOverwriteAttr(){
        EntityAttributes replacementAttr = new EntityAttributes(Map.of("created_at", "2021-10-01", "bar", "bat"));
        Entity preexisting = new Entity(testEntityId,
                testEntityType, new EntityAttributes(Map.of("created_at", "2022-10-01", "foo", "bar")));
        when(entityDao.getSingleEntity(any(), any(), any(), any())).thenReturn(preexisting)
                .thenReturn(new Entity(testEntityId,
                        testEntityType, replacementAttr));
        UUID workspaceId = UUID.randomUUID();

        ResponseEntity<EntityResponse> response = controller.putSingleEntity(workspaceId, "v0.2", testEntityType, testEntityId,
                new EntityRequest(testEntityId, testEntityType, replacementAttr));
//        verify(entityDao).replaceAllAttributes(preexisting, replacementAttr, workspaceId);

        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 2);
        assertEquals(replacementAttr, response.getBody().entityAttributes());
    }
    @Test
    void testPutNewSingleEntityWithReference() throws MissingReferencedTableException{
        when(entityDao.getReferenceCols(any(), any())).thenReturn(Collections.singletonList(new SingleTenantEntityReference("referencingAttr", testEntityType)));
        Map<String, Object> refAttr = new HashMap<>();
        refAttr.put("entityType", testEntityType.getName());
        refAttr.put("entityName", testEntityId.getEntityIdentifier());
        when(entityDao.getSingleEntity(any(), eq(new EntityType("referencingEntityType")), eq(new EntityId("referencingEntity")), any())).thenReturn(null)
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("referencingAttr", refAttr))));
        when(entityDao.getSingleEntity(any(), eq(testEntityType), eq(testEntityId), any()))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(new HashMap<>())));

        EntityAttributes referencingAttributes = new EntityAttributes(Map.of("referencingAttr", refAttr));

        EntityType referencingEntityType = new EntityType("referencingEntityType");
        EntityId referencingEntity = new EntityId("referencingEntity");

        UUID workspaceId = UUID.randomUUID();
        ResponseEntity<EntityResponse> response = controller.putSingleEntity(workspaceId, "v0.2", referencingEntityType, referencingEntity,
                new EntityRequest(referencingEntity, referencingEntityType, referencingAttributes));

//        verify(entityDao).createEntityType(workspaceId, Map.of("referencingAttr", DataTypeMapping.STRING), referencingEntityType.getName(), Set.of(new SingleTenantEntityReference("referencingAttr", testEntityType)));
//        verify(entityDao).createSingleEntity(workspaceId, referencingEntityType.getName(), new Entity(referencingEntity, referencingEntityType, referencingAttributes), new LinkedHashMap<>(Map.of("referencingAttr", DataTypeMapping.STRING)));

        assertTrue(response.getBody().entityAttributes().getAttributes().size() == 1, "Entity attributes should have 1 item");
        assertEquals(Map.of("referencingAttr", refAttr), response.getBody().entityAttributes().getAttributes(),
                "Entity attributes should contain referencingAttr");

        //Test Reference to Non-existent table
        doThrow(MissingReferencedTableException.class)
                .when(entityDao)
                .createEntityType(any(), any(), any(), any());
        response = controller.putSingleEntity(UUID.randomUUID(), "v0.2", referencingEntityType, referencingEntity,
                new EntityRequest(referencingEntity, referencingEntityType, referencingAttributes));
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode(), "Reference to non-existent table should return BAD_REQUEST");
    }

    @Test
    void testPutSingleEntityWithReference() throws MissingReferencedTableException, InvalidEntityReference {
        when(entityDao.getReferenceCols(any(), any())).thenReturn(Collections.singletonList(new SingleTenantEntityReference("referencingAttr", testEntityType)));
        when(entityDao.entityTypeExists(any(),any())).thenReturn(true);
        Map<String, Object> refAttr = new HashMap<>();
        refAttr.put("entityType", testEntityType.getName());
        refAttr.put("entityName", testEntityId.getEntityIdentifier());
        when(entityDao.getSingleEntity(any(), eq(new EntityType("referencingEntityType")), eq(new EntityId("referencingEntity")), any()))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(Map.of("foo", "bar"))));
        when(entityDao.getSingleEntity(any(), eq(testEntityType), eq(testEntityId), any()))
                .thenReturn(new Entity(testEntityId,
                        testEntityType, new EntityAttributes(new HashMap<>())));

        EntityAttributes referencingAttributes = new EntityAttributes(Map.of("referencingAttr", refAttr));

        EntityType referencingEntityType = new EntityType("referencingEntityType");
        EntityId referencingEntity = new EntityId("referencingEntity");

        UUID workspaceId = UUID.randomUUID();
        controller.putSingleEntity(workspaceId, "v0.2", referencingEntityType, referencingEntity,
                new EntityRequest(referencingEntity, referencingEntityType, referencingAttributes));

        //Should add column and foreign key for the new column
        verify(entityDao).addColumn(workspaceId, referencingEntityType.getName(), "referencingAttr", DataTypeMapping.STRING);
        verify(entityDao).addForeignKeyForReference(referencingEntityType.getName(), testEntityType.getName(), workspaceId, "referencingAttr");

        //Test Reference to Non-existent entity
        when(entityDao.getSingleEntity(any(), eq(testEntityType), eq(testEntityId), any()))
                .thenReturn(null);
        doThrow(InvalidEntityReference.class)
                .when(entityDao)
                .createSingleEntity(any(), any(), any(), any());
        ResponseEntity<EntityResponse> response = controller.putSingleEntity(UUID.randomUUID(), "v0.2", referencingEntityType, referencingEntity,
                    new EntityRequest(referencingEntity, referencingEntityType, referencingAttributes));
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode(), "Reference to Non-existent entity should return BAD_REQUEST");
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
        when(entityDao.workspaceSchemaExists(any())).thenReturn(true);
        when(entityDao.entityTypeExists(any(), any())).thenReturn(true);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> {
            controller.getSingleEntity(UUID.randomUUID(), "v0.2", testEntityType, testEntityId);
        });

        assertTrue(exception.getMessage().contains("Entity not found"));
        assertEquals(exception.getStatus(), HttpStatus.NOT_FOUND);
    }
}
