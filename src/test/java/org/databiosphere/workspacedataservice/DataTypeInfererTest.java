package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class DataTypeInfererTest {

    DataTypeInferer inferer = new DataTypeInferer();

    @Test
    void inferTypes() {
        Map<String, LinkedHashMap<String, DataTypeMapping>> result = inferer.inferTypes(getSomeEntityUpserts());
        assertThat(result.get("participants")).containsOnlyKeys("string_val", "int_val", "json_val", "date_val", "date_time_val");
        assertThat(result.get("participants").values())
                .containsExactlyInAnyOrder(DataTypeMapping.DATE, DataTypeMapping.JSON, DataTypeMapping.STRING, DataTypeMapping.LONG,
                        DataTypeMapping.DATE_TIME);
    }

    @Test
    void isValidJsonn(){
        assertThat(inferer.isValidJson(RandomStringUtils.randomNumeric(10))).isTrue();
        assertThat(inferer.isValidJson("Hello")).isFalse();
        assertThat(inferer.isValidJson(Boolean.TRUE.toString())).isTrue();
        assertThat(inferer.isValidJson("True")).isFalse();
    }

    @Test
    void inferSomeTypes(){
        assertThat(inferer.inferType("True")).isEqualTo(DataTypeMapping.BOOLEAN);
        assertThat(inferer.inferType("Hello")).isEqualTo(DataTypeMapping.STRING);
        assertThat(inferer.inferType("2020-01-01")).isEqualTo(DataTypeMapping.DATE);
        assertThat(inferer.inferType("2020-01-01T00:10:00")).isEqualTo(DataTypeMapping.DATE_TIME);
        assertThat(inferer.inferType("2020-01-01T00:10:00")).isEqualTo(DataTypeMapping.DATE_TIME);
        assertThat(inferer.inferType("12345")).isEqualTo(DataTypeMapping.LONG);
        assertThat(inferer.inferType("12345A")).isEqualTo(DataTypeMapping.STRING);
        assertThat(inferer.inferType("[\"12345\"]")).isEqualTo(DataTypeMapping.JSON);
    }

    private static List<EntityUpsert> getSomeEntityUpserts(){
        List<EntityUpsert> result = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            Entity e = new Entity(new EntityId("entity_"+i), new EntityType("participants"), new EntityAttributes(Map.of("int_val", new Random().nextInt(),
                    "string_val", RandomStringUtils.random(10), "json_val", "[\"a\", \"b\"]", "date_val", "2001-11-03", "date_time_val", "2001-11-03T10:00:00")));
            result.add(createUpsert(e));
        }
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
