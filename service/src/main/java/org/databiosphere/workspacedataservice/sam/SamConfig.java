package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean creator for:
 * - SamClientFactory, injecting the base url to Sam into that factory.
 * - SamDao, injecting the SamClientFactory into that dao.
 */
@Configuration
public class SamConfig {

    @Value("${SAM_URL:}")
    private String samUrl;

    private static final Logger LOGGER = LoggerFactory.getLogger(SamConfig.class);

    @Bean
    public SamClientFactory getSamClientFactory() {
        // TODO: fail if SAM_URL not present/valid
        LOGGER.info("Using Sam base url: '{}'", samUrl);
        return new HttpSamClientFactory(samUrl);
    }

    @Bean
    public SamDao samDao(SamClientFactory samClientFactory) {
        return new HttpSamDao(samClientFactory);
    }

}
