package org.databiosphere.workspacedataservice.dao;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockCloneDaoConfig {
    @Bean
    @Profile("mock-clone-dao")
    @Primary
    CloneDao mockCloneDao() {
        return new MockCloneDao();
    }
}
