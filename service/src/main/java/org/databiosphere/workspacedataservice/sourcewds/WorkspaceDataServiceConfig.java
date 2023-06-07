package org.databiosphere.workspacedataservice.sourcewds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkspaceDataServiceConfig {

        @Value("${workspacemanagerurl:}")
        private String workspaceDataServiceUrl;

        @Value("${twds.instance.workspace-id:}")
        private String workspaceId;

        private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceDataServiceConfig.class);

        @Bean
        public WorkspaceDataServiceClientFactory getWorkspaceDataServiceClientFactory() {
                LOGGER.info("Using workspace data service base url: '{}'", workspaceDataServiceUrl);
                return new HttpWorkspaceDataServiceClientFactory(workspaceDataServiceUrl);
        }

        @Bean
        public WorkspaceDataServiceDao workspaceManagerDao(WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory) {
                return new WorkspaceDataServiceDao(workspaceDataServiceClientFactory, workspaceId);
        }
}
