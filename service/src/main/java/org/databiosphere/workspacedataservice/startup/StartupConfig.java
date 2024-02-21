package org.databiosphere.workspacedataservice.startup;

import java.util.UUID;
import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** */
@Component
public class StartupConfig {

  private static final Logger logger = LoggerFactory.getLogger(StartupConfig.class);

  private final TwdsProperties twdsProperties;

  public StartupConfig(TwdsProperties twdsProperties) {
    this.twdsProperties = twdsProperties;
  }

  /**
   * Validates configuration properties on startup, to allow for failing fast in case of
   * misconfiguration.
   *
   * @param ignoredEvent the Spring event
   */
  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent ignoredEvent) {

    // validate single- vs. multi-tenancy.
    // if single-tenant, also validate the workspace id.
    if (twdsProperties.getTenancy() == null) {
      logger.warn(
          "twds.tenancy properties are not defined. "
              + "Was this deployment started with an active Spring profile of "
              + "either 'data-plane' or 'control-plane'?");
    } else {
      logger.info(
          "allow virtual collections: {}",
          twdsProperties.getTenancy().getAllowVirtualCollections());
      logger.info(
          "require $WORKSPACE_ID env var: {}",
          twdsProperties.getTenancy().getRequireEnvWorkspace());

      if (twdsProperties.getTenancy().getRequireEnvWorkspace()) {
        // attempt to parse the workspace id
        try {
          UUID workspaceUuid = UUID.fromString(twdsProperties.getInstance().getWorkspaceId());
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
  }
}
