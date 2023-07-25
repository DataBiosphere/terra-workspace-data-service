package org.databiosphere.workspacedataservice.dao;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cluster.HealthResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class OpenSearchDao {

    OpenSearchClient openSearchClient;

    public OpenSearchDao(OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    Logger logger = LoggerFactory.getLogger(this.getClass());

    public long count() {
        try {
            logger.info("about to query OpenSearch ... ");
            HealthResponse healthResponse = openSearchClient.cluster().health();
            logger.info("OpenSearch response received: {}", healthResponse);
            return healthResponse.activePrimaryShards();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }

}
