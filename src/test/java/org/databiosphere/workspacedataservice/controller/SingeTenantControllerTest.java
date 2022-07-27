package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
public class SingeTenantControllerTest {

    @Autowired
    private SingleTenantEntityController controller;

    private static UUID testWorkspace;

    @BeforeAll
    private static void createWorkspace(){
        testWorkspace = UUID.randomUUID();
    }

    @Test
    public void canCreateAndFetchNewEntity(){
        EntityUpsert eu = createEntityUpsert("sample", "samples", Map.of("participant_age", 18));
        ResponseEntity<String> response = controller.batchUpsert(testWorkspace, Collections.singletonList(eu));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        EntityQueryResult entityQueryResult = controller.queryForEntities(testWorkspace, "samples", 1, 10, "name", "asc", "", null);
        Entity actual = entityQueryResult.getResults().get(0);
        Map<String, Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put("participant_age", 18L);
        Entity expected = new Entity(new EntityId("sample"), new EntityType("samples"), new EntityAttributes(expectedAttributes));
        expected.setDeleted(false);
        assertThat(actual).isEqualTo(expected);
    }

    private EntityUpsert createEntityUpsert(String entityIdentifier, String entityType, Map<String, Object> entityAttributes) {
        EntityUpsert eu = new EntityUpsert();
        eu.setName(new EntityId(entityIdentifier));
        eu.setEntityType(entityType);
        List<UpsertOperation> ops = new ArrayList<>();
        for (String attrName : entityAttributes.keySet()) {
            UpsertOperation operation = new UpsertOperation(UpsertAction.AddUpdateAttribute, attrName, entityAttributes.get(attrName));
            ops.add(operation);
        }
        eu.setOperations(ops);
        return eu;
    }

    @Test
    public void canCreateAndDeleteNewEntity(){
        String entityName = "participant_1";
        String entityType = "participants";
        ResponseEntity<String> response = controller.batchUpsert(testWorkspace, Collections.singletonList(createEntityUpsert(entityName, entityType, Map.of("gender", "male"))));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        ResponseEntity<String> deleteResponse = controller.deleteEntities(testWorkspace, List.of(Map.of("entityType", entityType, "entityName", entityName)));
        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        EntityQueryResult queryForEntities = controller.queryForEntities(testWorkspace, entityType, 1, 10, "name", "asc", "", null);
        assertThat(queryForEntities.getResults().size()).isEqualTo(0);
    }


}
