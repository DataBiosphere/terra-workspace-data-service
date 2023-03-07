package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Bean-creator for SamClientFactory, injecting the base url to Sam into that factory.
 */
@Profile("!unit-test") // the "unit-test" profile creates mocks
@Configuration
public class SamClientFactoryConfig {

    @Value("${SAM_URL:}")
    private String samUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(SamClientFactoryConfig.class);

    @Bean
    public SamClientFactory getSamClientFactory() {
        // TODO: fail if SAM_URL not present/valid
        LOGGER.info("Using Sam base url: '{}'", samUrl);
        return new HttpSamClientFactory(samUrl);
    }

}
