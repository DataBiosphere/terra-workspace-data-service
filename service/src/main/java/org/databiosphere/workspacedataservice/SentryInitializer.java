package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@PropertySource("classpath:git.properties")
public class SentryInitializer  {
	private static final Logger LOGGER = LoggerFactory.getLogger(SentryInitializer.class);

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

	@Value("${sentry.dump-properties:false}")
	boolean dumpProperties;

	private static final Pattern SAM_ENV_PATTERN = Pattern.compile("\\.dsde-(\\p{Alnum}+)\\.");
	private static final String DEFAULT_ENV = "unknown";
	//Environments we want to monitor on sentry - don't send errors from local, bees, or Github actions
	private static final List<String> environments = List.of("prod", "alpha", "staging", "dev");

	@Bean
	public SmartInitializingSingleton initialize() {
		String env = urlToEnv(samurl);

		return () ->
        Sentry.init(options -> {
				options.setEnvironment(env);
				options.setDsn(environments.contains(env) ? dsn : "");
				options.setServerName(releaseName);
				options.setRelease(release);
				options.setTag("workspaceId", workspaceId);
				options.setTag("mrg", mrg);
				if (dumpProperties) {
					String summary =
							"Dumping Sentry properties, disable with sentry.dump-properties=false";
					String indentedNewline = "\n\t";
					String spacer = indentedNewline + "---------------------------------------" + indentedNewline;
					String logMessage = summary +
							spacer +
							Stream.of(
								String.format("env=%s", env),
								String.format("dsn=%s", dsn),
								String.format("serverName=%s", releaseName),
								String.format("release=%s", release),
								String.format("workspaceId=%s", workspaceId),
								String.format("mrg=%s", mrg)).collect(Collectors.joining(indentedNewline)) +
							spacer;
					LOGGER.info(logMessage);
				}
			});
	}

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
		} else {
			return DEFAULT_ENV;
		}
	}
}
