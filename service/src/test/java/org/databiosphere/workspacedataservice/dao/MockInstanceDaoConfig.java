package org.databiosphere.workspacedataservice.dao;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockInstanceDaoConfig {
    @Bean
    @Profile("mock-instance-dao")
    @Primary
    InstanceDao mockInstanceDao() {
        return new MockInstanceDao();
    }
}
