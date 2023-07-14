package org.databiosphere.workspacedataservice.dao;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockBackupRestoreDaoConfig {
    @Bean
    @Profile("mock-backup-dao")
    @Primary
    BackupRestoreDao mockBackupRestoreDao() {
        return new MockBackupRestoreDao();
    }
}
