package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:git.properties")
public class SentryInitializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryInitializer.class);

  @Value("${sentry.dsn}")
  String dsn;

  @Value("${twds.collection.workspace-id}")
  String workspaceId;

  @Value("${git.commit.id.abbrev}")
  String release;

  @Value("${samurl}")
  String samurl;

  @Value("${sentry.releasename}")
  String releaseName;

  @Value("${sentry.mrg}")
  String mrg;

  private static final Pattern SAM_ENV_PATTERN = Pattern.compile("\\.dsde-(\\p{Alnum}+)\\.");
  private static final String DEFAULT_ENV = "unknown";
  // Environments we want to monitor on sentry - don't send errors from local, bees, or Github
  // actions
  private static final List<String> environments = List.of("prod", "alpha", "staging", "dev");

  @Bean
  public SmartInitializingSingleton initialize() {
    String env = urlToEnv(samurl);

    return () ->
        Sentry.init(
            options -> {
              options.setEnvironment(env);
              options.setDsn(environments.contains(env) ? dsn : "");
              options.setServerName(releaseName);
              options.setRelease(release);
              options.setTag("workspaceId", workspaceId);
              options.setTag("mrg", mrg);
            });
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
