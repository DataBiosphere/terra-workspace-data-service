package org.databiosphere.workspacedataservice.storage;

import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class GcsStorageConfig {
  @Bean
  public GcsStorage GcsStorage(DataImportProperties properties) throws IOException {
    return new GcsStorage(properties);
  }
}
