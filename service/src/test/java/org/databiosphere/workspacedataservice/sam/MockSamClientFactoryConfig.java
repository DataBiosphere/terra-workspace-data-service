package org.databiosphere.workspacedataservice.sam;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;


@Configuration
public class MockSamClientFactoryConfig {

    // provide a MockSamClientFactory to unit tests marked with the "unit-test" profile.
    // marked as @Primary here to ensure it overrides the SamClientFactory provided
    // by the runtime SamClientFactoryConfig.
    @Bean
    @Profile("unit-test")
    @Primary
    public SamClientFactory getMockSamClientFactory() {
        return new MockSamClientFactory();
    }
}
