package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.spring.core.GcpProjectIdProvider;
import java.io.IOException;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GcsStorageConfig {

  @Bean
  public GcsStorage gcsStorage(
      GcpProjectIdProvider projectIdProvider, DataImportProperties properties) throws IOException {
    return GcsStorageImpl.create(projectIdProvider.getProjectId(), properties);
  }

}
