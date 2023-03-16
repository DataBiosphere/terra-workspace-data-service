package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.LogicalType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonConfig {
    @Bean
    public ObjectMapper objectMapper() {
        JsonMapper mapper = JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true)
                .findAndAddModules().build();
        mapper.coercionConfigFor(LogicalType.Boolean).setCoercion(CoercionInputShape.Integer, CoercionAction.Fail);
        mapper.coercionConfigFor(LogicalType.Float).setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        mapper.coercionConfigFor(LogicalType.Integer).setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        return mapper;
    }
}
