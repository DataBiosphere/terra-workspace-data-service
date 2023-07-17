package org.databiosphere.workspacedataservice.dao;


import org.databiosphere.workspacedataservice.shared.model.CloneTable;
import org.databiosphere.workspacedataservice.shared.model.RestoreResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class MockRestoreDaoConfig {
    @Bean
    @Profile("mock-restore-dao")
    @Primary
    BackupRestoreDao<RestoreResponse> mockRestoreDao() {
        return new MockBackupDao(CloneTable.RESTORE);
    }
}
