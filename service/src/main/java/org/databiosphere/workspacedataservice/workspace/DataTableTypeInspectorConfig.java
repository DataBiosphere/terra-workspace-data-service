package org.databiosphere.workspacedataservice.workspace;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataTableTypeInspectorConfig {

  @Bean
  @ControlPlane
  DataTableTypeInspector rawlsDataTableTypeInspector(RawlsClient rawlsClient) {
    return new RawlsDataTableTypeInspector(rawlsClient);
  }

  @Bean
  @DataPlane
  DataTableTypeInspector wdsDataTableTypeInspector() {
    return new WdsDataTableTypeInspector();
  }
}
