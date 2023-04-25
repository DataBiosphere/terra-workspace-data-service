package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstanceInitializerConfig {

    @Bean
    public InstanceInitializerBean instanceInitializerBean(InstanceDao instanceDao) {
        return new InstanceInitializerBean(instanceDao);
    }

    @Bean
    public ManagedIdentityDao managedIdentityDao() {
        return new ManagedIdentityDao();
    }

}
