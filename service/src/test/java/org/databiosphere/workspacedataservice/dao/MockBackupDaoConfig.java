package org.databiosphere.workspacedataservice.dao;


import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockBackupDaoConfig {
    @Bean
    @Profile("mock-backup-dao")
    @Primary
    BackupRestoreDao<BackupResponse> mockBackupDao() {
        return new MockBackupDao(CloneTable.BACKUP);
    }
}
