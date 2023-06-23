package org.databiosphere.workspacedataservice.dao;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class BackupDaoConfig {
    @Bean
    @Profile("test-back-dao")
    @Primary
    BackupDao BackupDao() {
        return null;//new PostgresBackupDao(null);
    }
}
