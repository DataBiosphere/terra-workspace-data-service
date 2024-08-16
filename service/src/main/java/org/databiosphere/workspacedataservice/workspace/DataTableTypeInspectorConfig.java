package org.databiosphere.workspacedataservice.workspace;

import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataTableTypeInspectorConfig {

  @Bean
  DataTableTypeInspector dataTableTypeInspector(RawlsClient rawlsClient) {
    return new RawlsDataTableTypeInspector(rawlsClient);
  }
}
