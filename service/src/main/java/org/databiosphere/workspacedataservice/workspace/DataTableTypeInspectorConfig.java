package org.databiosphere.workspacedataservice.workspace;

import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataTableTypeInspectorConfig {

  // RawlsDataTableTypeInspector will query Rawls for information
  // about the workspace and return the appropriate data table type based on the Rawls response.
  @Bean
  DataTableTypeInspector rawlsDataTableTypeInspector(
      RawlsClient rawlsClient, WorkspaceRepository workspaceRepository) {
    return new RawlsDataTableTypeInspector(rawlsClient, workspaceRepository);
  }
}
