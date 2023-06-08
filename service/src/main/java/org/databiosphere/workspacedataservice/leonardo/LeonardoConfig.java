package org.databiosphere.workspacedataservice.leonardo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LeonardoConfig {

        @Value("${leoUrl:}")
        private String leonardoUrl;

        @Value("${twds.instance.workspace-id:}")
        private String workspaceId;

        private static final Logger LOGGER = LoggerFactory.getLogger(LeonardoConfig.class);

        @Bean
        public LeonardoClientFactory getWorkspaceDataServiceClientFactory() {
                LOGGER.info("Using leonardo url: '{}'", leonardoUrl);
                return new HttpLeonardoClientFactory(leonardoUrl);
        }

        @Bean
        public LeonardoDao LeonardoDao(LeonardoClientFactory leonardoClientFactory) {
                return new LeonardoDao(leonardoClientFactory, workspaceId);
        }
}
