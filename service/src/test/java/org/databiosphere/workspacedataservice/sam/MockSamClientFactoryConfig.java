package org.databiosphere.workspacedataservice.sam;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


@Configuration
public class MockSamClientFactoryConfig {

//    private boolean errorOnResourceCreateDelete;
//    private boolean errorOnPermissionCheck;

    @Profile("unit-test & !errorOnPermissionCheck & !errorOnResourceCreateDelete")
    @Bean
    public SamClientFactory getMockSamClientFactory() {
        return new MockSamClientFactory(false, false);
    }

    @Profile("unit-test & !errorOnPermissionCheck & errorOnResourceCreateDelete")
    @Bean
    public SamClientFactory getErrorOnResourceCreateDeleteMockSamClientFactory() {
        return new MockSamClientFactory(false, true);
    }

    @Profile("unit-test & errorOnPermissionCheck & !errorOnResourceCreateDelete")
    @Bean
    public SamClientFactory getErrorOnPermissionCheckMockSamClientFactory() {
        return new MockSamClientFactory(true, false);
    }

    @Profile("unit-test & errorOnPermissionCheck & errorOnResourceCreateDelete")
    @Bean
    public SamClientFactory getErrorOnAllCallsMockSamClientFactory() {
        return new MockSamClientFactory(true, true);
    }

}
