package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired private Environment environment;

  @Value("${sentry.releasename}")
  String releaseName;

  @Value("${sentry.mrg}")
  String mrg;

  @Value("${sentry.env}")
  String terraEnv;

  @Value("${sentry.deploymentMode}")
  String deploymentMode;

  private static final Pattern SAM_ENV_PATTERN = Pattern.compile("\\.dsde-(\\p{Alnum}+)\\.");
  private static final String DEFAULT_ENV = "unknown";

  // Environments we want to monitor on sentry - don't send errors from local, bees, or Github
  // actions

  @Bean
  public SmartInitializingSingleton initialize() {
    String env;
    if (StringUtils.isNotBlank(terraEnv)) {
      env = terraEnv;
    } else {
      env = String.join(",", environment.getActiveProfiles());
    }

    return () ->
        Sentry.init(
            options -> {
              options.setEnvironment(env);
              options.setDsn(env != DEFAULT_ENV ? dsn : "");
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

  /**
   * Extracts an environment (e.g. "dev" or "prod") from a Sam url. Looks for ".dsde-${env} and
   * returns ${env} if found. Also looks for BEEs and returns the bee name if found. Else, returns
   * "unknown".
   *
   * @param samUrl the url to Sam
   * @return the environment as parsed from the Sam url
   */
  protected String urlToEnv(String samUrl) {
    if (samUrl == null) {
      return DEFAULT_ENV;
    }
    Matcher matcher = SAM_ENV_PATTERN.matcher(samUrl);
    boolean found = matcher.find();
    if (found) {
      return matcher.group(1);
    } else {
      return DEFAULT_ENV;
    }
  }
}
