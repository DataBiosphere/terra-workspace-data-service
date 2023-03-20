package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

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
        // TODO: AJ-898 what validation of the sam url should we do here?
        // - none
        // - check if the value is null/empty/whitespace
        // - check if the value is a valid Url
        // - contact the url and see if it looks like Sam on the other end
        // TODO: AJ-898 and what should we do if the validation fails?
        // - nothing, which would almost certainly result in Sam calls failing
        // - disable Sam integration, which could result in unauthorized access
        // - stop WDS, which would obviously prevent WDS from working at all
        LOGGER.info("Using Sam base url: '{}'", samUrl);
        return new HttpSamClientFactory(samUrl);
    }

    @Bean
    public SamDao samDao(SamClientFactory samClientFactory) {
        return new HttpSamDao(samClientFactory);
    }

}
