package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedataservice.dao.BackupDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstanceInitializerConfig {

    @Bean
    public InstanceInitializerBean instanceInitializerBean(InstanceDao instanceDao, BackupDao backupDao) {
        return new InstanceInitializerBean(instanceDao, backupDao);
    }
}
