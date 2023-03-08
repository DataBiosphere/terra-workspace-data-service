package org.databiosphere.workspacedataservice.sam;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * Bean-creator for MockSamClientFactory for use in unit tests.
 * <p>
 * Unit tests which would otherwise require a Sam instance to be running can activate
 * the "mock-sam" Spring profile to use these mock implementations instead, which:
 * - Always return true for all permission checks.
 * - Never throw any Exceptions.
 *
 * @see MockSamClientFactory
 * @see MockSamResourcesApi
 */
@Configuration
public class MockSamClientFactoryConfig {

    // provide a MockSamClientFactory to unit tests marked with the "mock-sam" profile.
    // marked as @Primary here to ensure it overrides the SamClientFactory provided
    // by the runtime SamClientFactoryConfig.
    @Bean
    @Profile("mock-sam")
    @Primary
    public SamClientFactory getMockSamClientFactory() {
        return new MockSamClientFactory();
    }
}
