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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
public class PatchSingleEntityTest {

    @MockBean
    EntityDao entityDao;


    @Test
    void testPatchSingleEntity(){
        when(entityDao.getSingleEntity(any(), any(), any())).thenReturn(new Entity("test",
                new EntityType("test-type"), new HashMap<>(Map.of("created_at", "2022-10-01")), 1L));
        EntityReferenceService referenceService = new EntityReferenceService(entityDao);
        EntityController controller = new EntityController(referenceService, entityDao);
        ResponseEntity<EntityResponse> response = controller.updateSingleEntity(UUID.randomUUID(), "v0.2", new EntityType("sample"), new EntityId("test"),
                new EntityRequest(new EntityId("test"), new EntityType("sample"), new EntityAttributes(Map.of("foo", "bar"))));
        assertTrue(response.getBody().entityAttributes().attributes().size() == 2);

    }
}
