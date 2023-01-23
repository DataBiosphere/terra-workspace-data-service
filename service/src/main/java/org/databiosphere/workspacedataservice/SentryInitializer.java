package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Configuration
@PropertySource("classpath:git.properties")
@PropertySource("classpath:application.properties")
public class SentryInitializer  {

	@Value("${sentry.dsn}")
	String dsn;

	@Value("${twds.instance.workspace-id}")
	String workspaceId;

	@Value("${git.commit.id.abbrev}")
	String release;

	@Value("${sentry.samurl}")
	String samurl;

	@Value("${sentry.releasename}")
	String releaseName;

	@Value("${sentry.mrg}")
	String mrg;

	@Bean
	public SmartInitializingSingleton initialize() {
		return () ->
        Sentry.init(options -> {
				options.setDsn(dsn);
				options.setEnvironment(urlToEnv(samurl));
				options.setServerName(releaseName);
				options.setRelease(release);
				options.setTag("workspaceId", workspaceId);
				options.setTag("mrg", mrg);
			});
	}

	private static final Pattern SAM_ENV_PATTERN = Pattern.compile("\\.dsde-(\\p{Alnum}+)\\.");
	private static final String DEFAULT_ENV = "unknown";

	/**
	 * Extracts an environment (e.g. "dev" or "prod") from a Sam url.
	 * Looks for ".dsde-${env} and returns ${env} if found.
	 * Also looks for BEEs and returns the bee name if found.
	 * Else, returns "unknown".
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
		} else if (samUrl.endsWith("bee.envs-terra.bio")) {
			return samUrl.split("\\.")[1];
		} else {
			return DEFAULT_ENV;
		}
	}
}
