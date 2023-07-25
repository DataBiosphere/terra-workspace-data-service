package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.CachedQueryDao;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.OpenSearchDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SortDirection;
import org.databiosphere.workspacedataservice.shared.model.TsvUploadResponse;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class OpenSearchService {

    private final OpenSearchDao openSearchDao;
    private final InstanceDao instanceDao;
    private final InstanceService instanceService;
    private final RecordDao recordDao;
    private final CachedQueryDao cachedQueryDao;
    private final RecordOrchestratorService recordOrchestratorService;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public OpenSearchService(OpenSearchDao openSearchDao, InstanceDao instanceDao,
                             InstanceService instanceService, RecordDao recordDao,
                             RecordOrchestratorService recordOrchestratorService,
                             CachedQueryDao cachedQueryDao) {
        this.openSearchDao = openSearchDao;
        this.instanceDao = instanceDao;
        this.instanceService = instanceService;
        this.recordDao = recordDao;
        this.recordOrchestratorService = recordOrchestratorService;
        this.cachedQueryDao = cachedQueryDao;
    }

    public int count() {
        long countLong = openSearchDao.count();
        return Long.valueOf(countLong).intValue();
    }

    public TsvUploadResponse reindex(UUID instanceId, RecordType recordType) {
        // TODO: create OpenSearch index if it does not exist
        logger.info("creating index {}", instanceId);

        openSearchDao.createIndex(instanceId, false); // will noop if already exists

        // TODO: delete type mappings if exist
        // TODO: create type mappings for record type
        // without creating explicit mappings, OpenSearch will infer mappings.
        // that's fine for now, but OpenSearch's inference may be different from WDS's
        // inference in certain cases

        String pk = cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId);
        int numRecords = recordDao.countRecords(instanceId, recordType);

        logger.info("will reindex {} records of type {}", numRecords, recordType.getName());

        BulkResponse.Builder aggregateResponseBuilder = new BulkResponse.Builder();

        long aggTook = 0;
        boolean anyError = false;
        List<BulkResponseItem> aggErrorItems = new ArrayList<BulkResponseItem>();

        // batch-iterate through Postgres records
        int batchSize = 500; // should be in config, not hardcoded?
        for (int offset = 0; offset <= numRecords; offset += batchSize) {
            logger.info("querying db for records with offset {} and limit {}", offset, batchSize);
            // call recordDao directly instead of calling recordOrchestratorService
            // to improve efficiency
            List<Record> recordBatch = recordDao.queryForRecords(recordType, batchSize,
                    offset, SortDirection.ASC.name().toLowerCase(),
                    pk, instanceId);

            // push to OpenSearch
            BulkResponse bulkResponse = openSearchDao.indexRecords(instanceId, recordType, recordBatch);

            if (bulkResponse.errors()) {
                anyError = true;
            }
            aggregateResponseBuilder.errors(anyError);
            aggTook += bulkResponse.took();
            aggregateResponseBuilder.took(aggTook);

            aggErrorItems.addAll(bulkResponse.items().stream().filter(x -> x.error() != null).toList());
            aggregateResponseBuilder.items(aggErrorItems);
        }

        BulkResponse aggResponse = aggregateResponseBuilder.build();

        String statusMessage = "Took: " + aggResponse.took() + "; error count: " + aggResponse.items().size();

        logger.info("reindex complete. " + statusMessage);

        return new TsvUploadResponse(numRecords, statusMessage);
    }

}
