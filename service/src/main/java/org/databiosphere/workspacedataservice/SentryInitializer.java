package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

@Configuration
@PropertySources({@PropertySource("classpath:git.properties"), @PropertySource("classpath:application.properties")})
public class SentryInitializer  {

	@Value("${sentry.dsn}")
	String dsn;

	@Value("${twds.instance.workspace-id}")
	String workspaceId;

	@Value("${git.commit.id.abbrev}")
	String release;

	@Value("${sentry.env:local}")
	String env;

	@Value("${sentry.servername:local-cluster}")
	String serverName;

	@Bean
	public SmartInitializingSingleton initialize() {
		return () -> {
        Sentry.init(options -> {
				options.setDsn(dsn);
				options.setEnvironment(env);
				options.setServerName(serverName);
				options.setRelease(release);
				options.setTag("workspaceId", workspaceId);
			});
		};
	}
}
