package org.databiosphere.workspacedataservice;

import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class DataTypeInfererTest {

    DataTypeInferer inferer = new DataTypeInferer();

    @Test
    void inferTypes() {
        Map<String, DataTypeMapping> result = inferer.inferTypes(getSomeAttrs());
        assertThat(result).containsOnlyKeys("string_val", "int_val", "json_val", "date_val", "date_time_val");
        assertThat(result.values())
                .containsExactlyInAnyOrder(DataTypeMapping.DATE, DataTypeMapping.JSON, DataTypeMapping.STRING, DataTypeMapping.LONG,
                        DataTypeMapping.DATE_TIME);
    }

    @Test
    void isValidJsonn(){
        assertThat(inferer.isValidJson(RandomStringUtils.randomNumeric(10))).isFalse();
        assertThat(inferer.isValidJson("Hello")).isFalse();
        assertThat(inferer.isValidJson(Boolean.TRUE.toString())).isFalse();
        assertThat(inferer.isValidJson("True")).isFalse();
        assertThat(inferer.isValidJson("{\"foo\":\"bar\"}")).isTrue();
    }

    @Test
    void inferSomeTypes(){
        assertThat(inferer.inferType("True")).isEqualTo(DataTypeMapping.BOOLEAN);
        assertThat(inferer.inferType("Hello")).isEqualTo(DataTypeMapping.STRING);
        assertThat(inferer.inferType("2020-01-01")).isEqualTo(DataTypeMapping.DATE);
        assertThat(inferer.inferType("2020-01-01T00:10:00")).isEqualTo(DataTypeMapping.DATE_TIME);
        assertThat(inferer.inferType("2020-01-01T00:10:00")).isEqualTo(DataTypeMapping.DATE_TIME);
        assertThat(inferer.inferType("12345")).isEqualTo(DataTypeMapping.STRING);
        assertThat(inferer.inferType("12345A")).isEqualTo(DataTypeMapping.STRING);
        assertThat(inferer.inferType("[\"12345\"]")).isEqualTo(DataTypeMapping.JSON);
    }

    private static Map<String, Object> getSomeAttrs(){
        return Map.of("int_val", new Random().nextInt(),
                "string_val", RandomStringUtils.random(10), "json_val", "[\"a\", \"b\"]", "date_val", "2001-11-03", "date_time_val", "2001-11-03T10:00:00");
    }


}
