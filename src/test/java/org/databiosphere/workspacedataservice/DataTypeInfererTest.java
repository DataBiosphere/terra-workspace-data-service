package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeInfererTest {

    DataTypeInferer inferer = new DataTypeInferer();

    @Test
    void inferTypes() {
        Map<String, Map<String, DataTypeMapping>> result = inferer.inferTypes(getSomeEntityUpserts());
        System.out.println("hello");
    }

    private static List<EntityUpsert> getSomeEntityUpserts(){
        List<EntityUpsert> result = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            Entity e = new Entity(new EntityId("entity_"+i), new EntityType("participants"), new EntityAttributes(Map.of("int_val", new Random().nextInt(),
                    "string_val", RandomStringUtils.random(10), "json_val", "[\"a\", \"b\"]", "date_val", "2001-11-03", "date_time_val", "2001-11-03T10:00:00")));
            result.add(createUpsert(e));
        }
        result.add(createUpsert(new Entity(new EntityId("entity_wild"), new EntityType("participants"), new EntityAttributes(Map.of("json_val", "foo")))));
        return result;
    }

    private static EntityUpsert createUpsert(Entity entity) {
        EntityUpsert upsert = new EntityUpsert();
        upsert.setName(entity.getName());
        upsert.setEntityType(entity.getEntityType().getName());
        upsert.setOperations(entity.getAttributes().getAttributes().entrySet().stream()
                .map(attr -> new UpsertOperation(UpsertAction.AddUpdateAttribute, attr.getKey(), attr.getValue()))
                .collect(Collectors.toList()));
        return upsert;
    }
}
