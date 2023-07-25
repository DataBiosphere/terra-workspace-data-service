package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.WaitForActiveShardOptions;
import org.opensearch.client.opensearch._types.WaitForActiveShards;
import org.opensearch.client.opensearch.cluster.HealthResponse;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

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

    public boolean indexExists(UUID instanceId) {
        ExistsRequest existsRequest = new ExistsRequest.Builder()
                .index(instanceId.toString())
                .build();

        return executeRequest(() ->
                openSearchClient.indices().exists(existsRequest).value());
    }

    public boolean createIndex(UUID instanceId, boolean throwIfExists) {
        if (!throwIfExists && indexExists(instanceId)) {
            return true;
        }

        WaitForActiveShards waitForActiveShards = new WaitForActiveShards.Builder()
                .option(WaitForActiveShardOptions.All)
                .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                .index(instanceId.toString())
                .waitForActiveShards(waitForActiveShards)
                        .build();

        return executeRequest(() ->
                openSearchClient.indices().create(createIndexRequest).acknowledged());
    }

    public BulkResponse indexRecords(UUID instanceId, RecordType recordType, List<Record> recordList) {

        List<BulkOperation> bulkOperations = recordList.stream()
                .map(x -> asBulkOperation(x, instanceId, recordType)).toList();

        // TODO: move away from WaitFor for better performance but eventual consistency
        BulkRequest bulkRequest = new BulkRequest.Builder()
                .refresh(Refresh.WaitFor)
                .operations(bulkOperations)
                        .build();

        BulkResponse bulkResponse = executeRequest(() ->
            openSearchClient.bulk(bulkRequest));

        logger.info("OpenSearch bulk operation complete. Has errors? {} took: {}",
                bulkResponse.errors(),
                bulkResponse.took());

        if (bulkResponse.errors()) {
            List<BulkResponseItem> errorItems = bulkResponse.items().stream().filter(x -> x.error() != null).toList();
            errorItems.forEach( item ->
                    logger.warn("Record {} error: {}", item.id(), Objects.requireNonNull(item.error()).reason())
            );
        }

        return bulkResponse;
    }

    private BulkOperation asBulkOperation(Record record, UUID instanceId, RecordType recordType) {
        Map<String, Object> sourceMap = record.getAttributes()
                .attributeSet().stream().collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        sourceMap.put("sys_recordtype", recordType.getName());

        IndexOperation<Map<String, ?>> indexOperation = new IndexOperation.Builder<Map<String, ?>>()
                .index(instanceId.toString())
                .document(sourceMap)
                .build();

        return new BulkOperation.Builder()
                .index(indexOperation).build();
    }

    private <T> T executeRequest (OpenSearchFunction<T> fn) {
        try {
            logger.info("about to query OpenSearch ... ");
            T response = fn.run();
            logger.info("query successful; returning " + response);
            return response;
        } catch (Exception e) {
            throw new RuntimeException("Error during OpenSearch request: " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    public interface OpenSearchFunction<T> {
        T run() throws Exception;
    }

}
