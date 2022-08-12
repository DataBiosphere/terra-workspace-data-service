package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityRequest;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference.ENTITY_NAME_KEY;
import static org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference.ENTITY_TYPE_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class EntityControllerMockMvcTest {

    private final ObjectMapper mapper = new ObjectMapper();
    @Autowired
    private MockMvc mockMvc;

    private static UUID instanceId;

    private static String versionId = "v0.2";

    @BeforeAll
    private static void createWorkspace(){
        instanceId = UUID.randomUUID();
    }


    @Test
    public void createInstanceAndTryToFetchMissingEntity() throws Exception {
        UUID uuid = UUID.randomUUID();
        mockMvc.perform(post("/{instanceId}/{version}/", uuid, versionId))
                .andExpect(status().isCreated());
        mockMvc.perform(post("/{instanceId}/{version}/", uuid, versionId))
                .andExpect(status().isConflict());
    }


    @Test
    public void tryFetchingMissingEntityType() throws Exception {
        mockMvc.perform(get("/{instanceId}/entities/{versionId}/{entityType}/{entityId}", instanceId, versionId, "missing", "missing-2"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void tryFetchingMissingEntity() throws Exception {
        String entityType1 = "entityType1";
        createSomeEntities(entityType1, 1);
        mockMvc.perform(get("/{instanceId}/entities/{versionId}/{entityType}/{entityId}", instanceId, versionId, entityType1, "missing-2"))
                .andExpect(status().isNotFound());
    }

    @Test
    public void createAndRetrieveEntity() throws Exception {
        String entityType = "samples";
        createSomeEntities(entityType, 1);
        mockMvc.perform(get("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, entityType, "entity_0"))
                .andExpect(status().isOk());
    }

    @Test
    public void createEntityWithReferences() throws Exception {
        String referencedType = "ref_participants";
        String referringType = "ref_samples";
        createSomeEntities(referencedType, 3);
        createSomeEntities(referringType, 1);
        Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> ref = new HashMap<>();
        ref.put(ENTITY_TYPE_KEY, referencedType);
        ref.put(ENTITY_NAME_KEY, "entity_0");
        attributes.put("sample-ref", ref);
        mockMvc.perform(patch("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, referringType, "entity_0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(
                                new EntityRequest(new EntityId("entity_0"), new EntityType(referringType), new EntityAttributes(attributes)))))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("\""+ENTITY_NAME_KEY+"\":\"entity_0\"")));;
    }

    @Test
    public void referencingMissingTableFails() throws Exception {
        String referencedType = "missing";
        String referringType = "ref_samples-2";
        createSomeEntities(referringType, 1);
        Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> ref = new HashMap<>();
        ref.put(ENTITY_TYPE_KEY, referencedType);
        ref.put(ENTITY_NAME_KEY, "entity_0");
        attributes.put("sample-ref", ref);
        mockMvc.perform(put("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, referringType, "entity_0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(
                                new EntityRequest(new EntityId("entity_0"), new EntityType(referringType), new EntityAttributes(attributes)))))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(containsString("It looks like you're attempting to assign a reference to a table, missing, that does not exist")));;
    }

    @Test
    public void referencingMissingEntityFails() throws Exception {
        String referencedType = "ref_participants-2";
        String referringType = "ref_samples-3";
        createSomeEntities(referencedType, 3);
        createSomeEntities(referringType, 1);
        Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> ref = new HashMap<>();
        ref.put(ENTITY_TYPE_KEY, referencedType);
        ref.put(ENTITY_NAME_KEY, "entity_99");
        attributes.put("sample-ref", ref);
        mockMvc.perform(put("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, referringType, "entity_0")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(
                                new EntityRequest(new EntityId("entity_0"), new EntityType(referringType), new EntityAttributes(attributes)))))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(containsString("It looks like you're trying to reference an entity that does not exist.")));
    }

    @Test
    public void expandColumnDefForNewData() throws Exception {
        String entityType = "to-alter";
        createSomeEntities(entityType, 1);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("attr3", "convert this column from date to text");
        mockMvc.perform(put("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, entityType, "entity_1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(
                                new EntityRequest(new EntityId("entity_0"), new EntityType(entityType), new EntityAttributes(attributes)))))
                .andExpect(status().isCreated());
    }

    @Test
    public void patchMissingEntity() throws Exception {
        String entityType = "to-patch";
        createSomeEntities(entityType, 1);
        Map<String, Object> entityAttributes = new HashMap<>();
        entityAttributes.put("attr-boolean", true);
        String entityId = "entity_missing";
        mockMvc.perform(patch("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, entityType, entityId)
                        .content(mapper.writeValueAsString(new EntityRequest(new EntityId(entityId), new EntityType(entityType), new EntityAttributes(entityAttributes))))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());
    }

    @Test
    public void putEntityWithMissingTableReference() throws Exception{
        String entityType = "entity-type-missing-table-ref";
        String entityId = "entity_0";
        Map<String, Object> entityAttributes = new HashMap<>();
        Map<String, Object> ref = new HashMap<>();
        ref.put(ENTITY_TYPE_KEY, "missing");
        ref.put(ENTITY_NAME_KEY, "missing_also");
        entityAttributes.put("sample-ref", ref);

        mockMvc.perform(put("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, entityType, entityId)
                        .content(mapper.writeValueAsString(new EntityRequest(new EntityId(entityId), new EntityType(entityType), new EntityAttributes(entityAttributes))))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(containsString("assign a reference to a table that does not exist")));
    }

    @Test
    public void tryToAssignReferenceToNonRefColumn() throws Exception {
        String entityType = "ref-alter";
        createSomeEntities(entityType, 1);
        Map<String, Object> entityAttributes = new HashMap<>();
        Map<String, Object> ref = new HashMap<>();
        ref.put(ENTITY_TYPE_KEY, "missing");
        ref.put(ENTITY_NAME_KEY, "missing_also");
        entityAttributes.put("attr1", ref);
        mockMvc.perform(patch("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, entityType, "entity_0")
                .content(mapper.writeValueAsString(new EntityRequest(new EntityId("entity_0"), new EntityType(entityType), new EntityAttributes(entityAttributes))))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isConflict())
                .andExpect(result -> assertTrue(result.getResolvedException().getMessage().contains("reference to an existing column that was not configured for references")));
    }

    private void createSomeEntities(String entityType, int numEntities) throws Exception {
        for (int i = 0; i < numEntities; i++) {
            String entityId = "entity_" + i;
            Map<String, Object> entityAttributes = new HashMap<>();
            entityAttributes.put("attr1", RandomStringUtils.randomAlphabetic(6));
            entityAttributes.put("attr2", RandomUtils.nextFloat());
            entityAttributes.put("attr3", "2022-11-01");
            entityAttributes.put("attr4", RandomStringUtils.randomNumeric(5));
            entityAttributes.put("attr5", RandomUtils.nextLong());
            entityAttributes.put("attr-dt", "2022-03-01T12:00:03");
            entityAttributes.put("attr-json", "{\"foo\":\"bar\"}");
            entityAttributes.put("attr-boolean", true);
            mockMvc.perform(put("/{instanceId}/entities/{version}/{entityType}/{entityId}", instanceId, versionId, entityType, entityId)
                            .content(mapper.writeValueAsString(new EntityRequest(new EntityId(entityId), new EntityType(entityType), new EntityAttributes(entityAttributes))))
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isCreated());
        }
    }
}
