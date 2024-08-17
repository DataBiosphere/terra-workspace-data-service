package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.spring.core.GcpProjectIdProvider;
import java.io.IOException;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GcsStorageConfig {

  @ControlPlane
  @Bean
  public GcsStorage gcsStorage(
      GcpProjectIdProvider projectIdProvider, DataImportProperties properties) throws IOException {
    return GcsStorageImpl.create(projectIdProvider.getProjectId(), properties);
  }

  @ConditionalOnMissingBean
  @DataPlane
  @Bean
  public GcsStorage noopGcsStorage() {
    return new GcsStorageNoopImpl();
  }
}
