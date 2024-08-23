package org.databiosphere.workspacedataservice.startup;

import static java.util.Arrays.stream;

import java.util.Set;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/** */
@Component
public class StartupConfig {
  private static final Set<String> REQUIRED_PROFILES = Set.of("data-plane", "control-plane");
  private static final Logger logger = LoggerFactory.getLogger(StartupConfig.class);

  private final TenancyProperties tenancyProperties;
  private final Environment environment;
  @Nullable private WorkspaceId workspaceId;

  public StartupConfig(Environment environment, TenancyProperties tenancyProperties) {
    this.tenancyProperties = tenancyProperties;
    this.environment = environment;
  }

  @Autowired(required = false) // control plane won't have workspaceId
  void setWorkspaceId(@Nullable @SingleTenant WorkspaceId workspaceId) {
    this.workspaceId = workspaceId;
  }

  /**
   * Validates configuration properties on startup, to allow for failing fast in case of
   * misconfiguration.
   *
   * @param ignoredEvent the Spring event
   */
  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent ignoredEvent) {
    assertDeploymentModeProfileEnabled();
    logger.info("require $WORKSPACE_ID env var: {}", tenancyProperties.getRequireEnvWorkspace());

    if (tenancyProperties.getRequireEnvWorkspace()) {
      if (workspaceId == null) {
        throw new ConfigurationException("This deployment requires a valid $WORKSPACE_ID env var.");
      }
      logger.info("single-tenant workspace id: {}", workspaceId);
    }
  }

  private void assertDeploymentModeProfileEnabled() {
    boolean matchingProfileFound =
        stream(environment.getActiveProfiles()).anyMatch(REQUIRED_PROFILES::contains);

    if (!matchingProfileFound) {
      throw new ConfigurationException(
          "This deployment requires at least one of the following active profiles: %s"
              .formatted(REQUIRED_PROFILES));
    }
  }
}
