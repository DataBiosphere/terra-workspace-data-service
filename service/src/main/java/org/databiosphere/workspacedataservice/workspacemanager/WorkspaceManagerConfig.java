package org.databiosphere.workspacedataservice.workspacemanager;

import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkspaceManagerConfig {

  @Value("${workspacemanagerurl:}")
  private String workspaceManagerUrl;

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceManagerConfig.class);

  @Bean
  public WorkspaceManagerClientFactory getWorkspaceManagerClientFactory() {
    LOGGER.info("Using workspace manager base url: '{}'", workspaceManagerUrl);
    return new HttpWorkspaceManagerClientFactory(workspaceManagerUrl);
  }

  @Bean
  public WorkspaceManagerDao workspaceManagerDao(
      WorkspaceManagerClientFactory workspaceManagerClientFactory,
      RestClientRetry restClientRetry) {
    return new WorkspaceManagerDao(workspaceManagerClientFactory, restClientRetry);
  }
}
