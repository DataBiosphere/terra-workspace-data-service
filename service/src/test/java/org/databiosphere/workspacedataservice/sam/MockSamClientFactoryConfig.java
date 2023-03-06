package org.databiosphere.workspacedataservice.sam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("unit-test")
@Configuration
public class MockSamClientFactoryConfig {

    @Bean
    public SamClientFactory getSamClientFactory() {
        return new MockSamClientFactory();
    }

}
