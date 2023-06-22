package org.databiosphere.workspacedataservice.storage;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
public class LocalFileStorageConfig {
    @Bean
    @Profile("mock-storage")
    BackUpFileStorage mockStorage() {
        return new LocalFileStorage();
    }
}
