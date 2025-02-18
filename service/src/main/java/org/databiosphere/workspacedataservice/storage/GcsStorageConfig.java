package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.spring.core.GcpProjectIdProvider;
import java.io.IOException;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GcsStorageConfig {

  @ConditionalOnProperty(name = "spring.cloud.gcp.core.enabled", havingValue = "true")
  @Bean
  public GcsStorage gcsStorage(
      GcpProjectIdProvider projectIdProvider, DataImportProperties properties) throws IOException {
    return GcsStorageImpl.create(projectIdProvider.getProjectId(), properties);
  }

  // to support Python tests
  @ConditionalOnProperty(name = "spring.cloud.gcp.core.enabled", havingValue = "false")
  @Bean
  public GcsStorage noopGcsStorage() {
    return new GcsStorageNoopImpl();
  }
}
