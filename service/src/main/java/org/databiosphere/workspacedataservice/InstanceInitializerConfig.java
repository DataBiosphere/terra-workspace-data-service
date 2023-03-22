package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstanceInitializerConfig {

    @Bean
    public InstanceInitializerBean instanceInitializerBean(SamDao samDao, InstanceDao instanceDao, ManagedIdentityDao managedIdentityDao) {
        return new InstanceInitializerBean(samDao, instanceDao, managedIdentityDao);
    }

    @Bean
    public ManagedIdentityDao managedIdentityDao() {
        return new ManagedIdentityDao();
    }

}
