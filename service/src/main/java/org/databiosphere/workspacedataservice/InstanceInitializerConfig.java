package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.ManagedIdentityDao;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstanceInitializerConfig {

    @Bean
    public InstanceInitializerBean instanceInitializerBean(InstanceDao instanceDao, LeonardoDao leoDao, BackupDao backupDao) {
        return new InstanceInitializerBean(instanceDao, leoDao, backupDao);
    }

    @Bean
    public ManagedIdentityDao managedIdentityDao() {
        return new ManagedIdentityDao();
    }

}
