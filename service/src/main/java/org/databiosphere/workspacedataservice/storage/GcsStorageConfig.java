package org.databiosphere.workspacedataservice.storage;

import java.io.IOException;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GcsStorageConfig {
  @Bean
  public GcsStorage GcsStorage(DataImportProperties properties) throws IOException {
    return new GcsStorage(properties);
  }
}
