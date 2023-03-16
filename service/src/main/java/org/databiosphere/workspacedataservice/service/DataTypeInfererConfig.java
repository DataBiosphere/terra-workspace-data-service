package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataTypeInfererConfig {
    @Bean
    public DataTypeInferer inferer(ObjectMapper mapper){
        return new DataTypeInferer(mapper);
    }
}
