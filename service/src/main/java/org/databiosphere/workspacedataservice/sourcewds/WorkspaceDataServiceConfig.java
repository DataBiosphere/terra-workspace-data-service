package org.databiosphere.workspacedataservice.sourcewds;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkspaceDataServiceConfig {

  @Bean
  public WorkspaceDataServiceClientFactory getWorkspaceDataServiceClientFactory() {
    return new HttpWorkspaceDataServiceClientFactory();
  }

  @Bean
  public WorkspaceDataServiceDao workspaceDataServiceDao(
      WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory,
      RestClientRetry restClientRetry) {
    return new WorkspaceDataServiceDao(workspaceDataServiceClientFactory, restClientRetry);
  }
}
