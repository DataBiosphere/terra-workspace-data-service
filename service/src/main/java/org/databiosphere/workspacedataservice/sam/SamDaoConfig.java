package org.databiosphere.workspacedataservice.sam;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean-creator for SamDao, injecting a SamClientFactory into that dao.
 */
@Configuration
public class SamDaoConfig {

    private SamClientFactory samClientFactory;

    public SamDaoConfig(SamClientFactory samClientFactory) {
        this.samClientFactory = samClientFactory;
    }

    @Bean
    public SamDao samDao() {
        return new HttpSamDao(samClientFactory);
    }



}
