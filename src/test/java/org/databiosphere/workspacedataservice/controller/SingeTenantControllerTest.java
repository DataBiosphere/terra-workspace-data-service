package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
        String entityIdentifier = "sample";
        String entityType = "samples-foo";
        String attrName = "participant-age";
        createOrUpdateEntity(entityIdentifier, entityType, Map.of(attrName, 18));
        EntityQueryResult entityQueryResult = controller.queryForEntities(testWorkspace, entityType, 1, 10, "sys_name", "asc", "", null);
        Entity actual = entityQueryResult.getResults().get(0);
        Map<String, Object> expectedAttributes = new HashMap<>();
        expectedAttributes.put(attrName, 18L);
        Entity expected = new Entity(new EntityId(entityIdentifier), new EntityType(entityType), new EntityAttributes(expectedAttributes));
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
        createOrUpdateEntity(entityName, entityType, Map.of("gender", "male"));
        ResponseEntity<String> deleteResponse = controller.deleteEntities(testWorkspace, List.of(Map.of("entityType", entityType, "entityName", entityName)));
        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        EntityQueryResult queryForEntities = controller.queryForEntities(testWorkspace, entityType, 1, 10, "sys_name", "asc", "", null);
        assertThat(queryForEntities.getResults().size()).isEqualTo(0);
    }

    private void createOrUpdateEntity(String entityName, String entityType, Map<String, Object> attributes){
        ResponseEntity<String> response = controller.batchUpsert(testWorkspace, Collections.singletonList(createEntityUpsert(entityName, entityType, attributes)));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    public void canUpdateExistingEntityAndType(){
        createOrUpdateEntity("Sub 1", "Submissions", Map.of("submission-time", "2022-07-01"));
        createOrUpdateEntity("Sub 1", "Submissions", Map.of("completion-time", "2022-07-02"));
        createOrUpdateEntity("Sub 2", "Submissions for Mom", Map.of("completion-time", "2022-07-11"));
    }

}
