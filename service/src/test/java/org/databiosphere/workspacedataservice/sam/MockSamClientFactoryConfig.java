package org.databiosphere.workspacedataservice.sam;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


@Configuration
@Profile("unit-test")
public class MockSamClientFactoryConfig {

    @Bean
    public SamClientFactory getMockSamClientFactory() {
        return new MockSamClientFactory();
    }
}
