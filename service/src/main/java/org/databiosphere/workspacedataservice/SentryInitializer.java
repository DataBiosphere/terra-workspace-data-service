package org.databiosphere.workspacedataservice;

import io.sentry.Sentry;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

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

	@Value("${sentry.servername}")
	String serverName;

	@Bean
	public SmartInitializingSingleton initialize() {
		return () ->
        Sentry.init(options -> {
				options.setDsn(dsn);
				options.setEnvironment(urlToEnv(samurl));
				options.setServerName(serverName);
				options.setRelease(release);
				options.setTag("workspaceId", workspaceId);
			});
	}

	String urlToEnv(String samUrl){
		String env = "dev";
		if (samUrl != null){
			int dsde_loc = samUrl.indexOf("dsde-");
			int broad_loc = samUrl.indexOf(".broad");
			if (dsde_loc > -1 && broad_loc > -1 && dsde_loc != broad_loc){
				env = samUrl.substring(dsde_loc+5,broad_loc);
			}
		}
		return env;
	}
}
