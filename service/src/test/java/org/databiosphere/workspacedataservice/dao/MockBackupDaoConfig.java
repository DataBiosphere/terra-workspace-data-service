package org.databiosphere.workspacedataservice.dao;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockBackupDaoConfig {
    @Bean
    @Profile("mock-backup-dao")
    @Primary
    BackupRestoreDao mockBackupDao() {
        return new MockBackupDao();
    }
}
