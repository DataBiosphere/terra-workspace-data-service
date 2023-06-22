package org.databiosphere.workspacedataservice.sourcewds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkspaceDataServiceConfig {

        @Value("${twds.instance.workspace-id:}")
        private String workspaceId;

        private static final Logger LOGGER = LoggerFactory.getLogger(WorkspaceDataServiceConfig.class);

        public WorkspaceDataServiceClientFactory getWorkspaceDataServiceClientFactory(String wdsEndpoint) {
                LOGGER.info("Using workspace data service base url: '{}'", wdsEndpoint);
                return new HttpWorkspaceDataServiceClientFactory(wdsEndpoint);
        }

        public WorkspaceDataServiceDao workspaceManagerDao(WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory) {
                return new WorkspaceDataServiceDao(workspaceDataServiceClientFactory, workspaceId);
        }
}
