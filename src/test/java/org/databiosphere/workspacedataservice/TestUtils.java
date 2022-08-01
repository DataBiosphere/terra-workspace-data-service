package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityUpsert;
import org.databiosphere.workspacedataservice.shared.model.UpsertAction;
import org.databiosphere.workspacedataservice.shared.model.UpsertOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestUtils {

    public static EntityUpsert createEntityUpsert(String entityIdentifier, String entityType, Map<String, Object> entityAttributes) {
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
}
