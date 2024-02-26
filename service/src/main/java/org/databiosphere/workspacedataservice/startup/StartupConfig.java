package org.databiosphere.workspacedataservice.startup;

import static java.util.Arrays.stream;

import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.databiosphere.workspacedataservice.config.InstanceProperties;
import org.databiosphere.workspacedataservice.config.TenancyProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** */
@Component
public class StartupConfig {
  private static final Set<String> REQUIRED_PROFILES = Set.of("data-plane", "control-plane");
  private static final Logger logger = LoggerFactory.getLogger(StartupConfig.class);

  private final InstanceProperties instanceProperties;
  private final TenancyProperties tenancyProperties;
  private final Environment environment;

  public StartupConfig(
      Environment environment,
      InstanceProperties instanceProperties,
      TenancyProperties tenancyProperties) {
    this.instanceProperties = instanceProperties;
    this.tenancyProperties = tenancyProperties;
    this.environment = environment;
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
    logger.info("allow virtual collections: {}", tenancyProperties.getAllowVirtualCollections());
    logger.info("require $WORKSPACE_ID env var: {}", tenancyProperties.getRequireEnvWorkspace());

    if (tenancyProperties.getRequireEnvWorkspace()) {
      // attempt to parse the workspace id
      try {
        UUID workspaceUuid = UUID.fromString(instanceProperties.getWorkspaceId());
        logger.info("single-tenant workspace id: {}", workspaceUuid);
      } catch (Exception e) {
        throw new ConfigurationException(
            "This deployment requires a $WORKSPACE_ID env var, but its value "
                + "could not be parsed to a UUID: "
                + e.getMessage(),
            e);
      }
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
