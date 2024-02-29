package org.databiosphere.workspacedataservice.storage;

import java.io.IOException;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ControlPlane
public class GcsStorageConfig {
  @Bean
  public GcsStorage gcsStorage(DataImportProperties properties) throws IOException {
    return new GcsStorage(properties);
  }
}
