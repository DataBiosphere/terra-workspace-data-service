package org.databiosphere.workspacedataservice.storage;


import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration

public class AzureBlobStorageConfig {

    private final WorkspaceManagerDao workspaceManagerDao;

    public AzureBlobStorageConfig(WorkspaceManagerDao workspaceManagerDao) {
        this.workspaceManagerDao = workspaceManagerDao;
    }

    @Bean
    //@Profile("storage")
    BackUpFileStorage Storage() {
        return new AzureBlobStorage(workspaceManagerDao);
    }
}
