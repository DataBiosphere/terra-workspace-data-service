package org.databiosphere.workspacedataservice.dao;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class InstanceDaoConfig {
    @Bean
    @Profile("test-instance-dao")
    @Primary
    InstanceDao InstanceDao() {
        return null; //new PostgresInstanceDao(null);
    }
}
