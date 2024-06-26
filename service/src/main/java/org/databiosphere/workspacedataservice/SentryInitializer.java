package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("classpath:git.properties")
public class SentryInitializer {
  @Value("${sentry.dsn}")
  String dsn;

  @Value("${twds.instance.workspace-id}")
  String workspaceId;

  @Value("${git.commit.id.abbrev}")
  String release;

  @Value("${sentry.releasename}")
  String releaseName;

  @Value("${sentry.mrg}")
  String mrg;

  @Value("${sentry.env}")
  String terraEnv;

  @Value("${sentry.deploymentMode}")
  String deploymentMode;

  private static final String DEFAULT_ENV = "unknown";

  // Environments we want to monitor on sentry - don't send errors from local, bees, or Github
  // actions
  private static final List<String> sentryEnvironments = List.of("prod", "staging", "dev");

  @Bean
  public SmartInitializingSingleton initialize(Environment environment) {
    var env = getSentryEnvironment(environment.getActiveProfiles()).orElse(DEFAULT_ENV);

    return () ->
        Sentry.init(
            options -> {
              options.setEnvironment(env);
              options.setDsn(env.equals(DEFAULT_ENV) ? "" : dsn);
              options.setServerName(releaseName);
              options.setRelease(release);
              // additional tags:
              getTags().forEach(options::setTag);
            });
  }

  /**
   * Conditionally add tags; don't add any that are blank.
   *
   * @return tags to send to Sentry
   */
  Map<String, String> getTags() {
    Map<String, String> tags = new HashMap<>();
    // workspaceId, used in data plane
    if (StringUtils.isNotBlank(workspaceId)) {
      tags.put("workspaceId", workspaceId);
    }
    // MRG, used in data plane
    if (StringUtils.isNotBlank(mrg)) {
      tags.put("mrg", mrg);
    }
    // deploymentMode, should be present in both control plane and data plane
    if (StringUtils.isNotBlank(deploymentMode)) {
      tags.put("deploymentMode", deploymentMode);
    }
    return tags;
  }

  Optional<String> getSentryEnvironment(String[] profiles) {
    if (StringUtils.isNotBlank(terraEnv)) {
      if (sentryEnvironments.contains(terraEnv)) {
        return Optional.of(terraEnv);
      }
    } else {
      for (var profile : profiles) {
        if (sentryEnvironments.contains(profile)) {
          return Optional.of(profile);
        }
      }
    }
    return Optional.empty();
  }
}
