package org.databiosphere.workspacedataservice.dao;

import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.databiosphere.workspacedataservice.shared.model.SortDirection;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldSort;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.WaitForActiveShardOptions;
import org.opensearch.client.opensearch._types.WaitForActiveShards;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.cluster.HealthResponse;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.DeleteByQueryRequest;
import org.opensearch.client.opensearch.core.DeleteByQueryResponse;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
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

    public final String FIELD_RECORDTYPE = "sys_recordtype";

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

    public SearchResponse<Object> search(UUID instanceId, RecordType recordType,
                                               SearchRequest searchRequest) {
        // sorting
        SortOrder sortOrder = searchRequest.getSort().equals(SortDirection.DESC) ? SortOrder.Desc : SortOrder.Asc;
        FieldSort fieldSort = new FieldSort.Builder()
                .field(searchRequest.getSortAttribute())
                .order(sortOrder)
                .build();
        SortOptions sortOptions = new SortOptions.Builder()
                .field(fieldSort)
                .build();

        // filter to supplied record type
        Query recordTypeQuery = recordTypeQuery(recordType);

        org.opensearch.client.opensearch.core.SearchRequest openSearchRequest = new org.opensearch.client.opensearch.core.SearchRequest
                        .Builder()
                .index(instanceId.toString())
                // .sort(sortOptions) // TODO: sorting requires mappings
                .from(searchRequest.getOffset())
                .size(searchRequest.getLimit())
                .query(recordTypeQuery)
                .build();

        SearchResponse<Object> searchResponse = executeRequest(() ->
                openSearchClient.search(openSearchRequest, Object.class));

        logger.info("search took: " + searchResponse.took());

        return searchResponse;
    }

    public void deleteRecordType(UUID instanceId, RecordType recordType) {
        // filter to supplied record type
        Query recordTypeQuery = recordTypeQuery(recordType);

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest.Builder()
                .index(instanceId.toString())
                .query(recordTypeQuery)
                .build();
        DeleteByQueryResponse deleteByQueryResponse = executeRequest(() ->
                openSearchClient.deleteByQuery(deleteByQueryRequest));

        long failureSize = deleteByQueryResponse.failures().size();

        logger.info("deleted {} records of type {} in {}. Failures: {}",
                deleteByQueryResponse.deleted(),
                recordType.getName(),
                deleteByQueryResponse.took(),
                failureSize);

        deleteByQueryResponse.failures().forEach( failure -> {
            logger.warn("delete failure for id " + failure.id() + ": " + failure.cause().reason());
        });
    }

    private Query recordTypeQuery(RecordType recordType) {
        TermQuery termQuery = new TermQuery.Builder()
                .field(FIELD_RECORDTYPE)
                .value(new FieldValue.Builder().stringValue(recordType.getName()).build())
                .build();
        BoolQuery boolQuery = new BoolQuery.Builder()
                .must(termQuery._toQuery()).build();
        return boolQuery._toQuery();
    }


    private BulkOperation asBulkOperation(Record record, UUID instanceId, RecordType recordType) {
        Map<String, Object> sourceMap = record.getAttributes()
                .attributeSet().stream().collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        sourceMap.put(FIELD_RECORDTYPE, recordType.getName());

        IndexOperation<Map<String, ?>> indexOperation = new IndexOperation.Builder<Map<String, ?>>()
                .index(instanceId.toString())
                .id(record.getId())
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
