package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.ReadTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

@Service
public class RecordOrchestratorService { // TODO give me a better name

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordOrchestratorService.class);
    private static final int MAX_RECORDS = 1_000;

    private final RecordDao recordDao;
    private final BatchWriteService batchWriteService;
    private final RecordService recordService;
    private final InstanceService instanceService;
    private final ObjectMapper objectMapper;
    private final SamDao samDao;
    private final ActivityLogger activityLogger;

    public RecordOrchestratorService(RecordDao recordDao,
                                     BatchWriteService batchWriteService,
                                     RecordService recordService,
                                     InstanceService instanceService,
                                     ObjectMapper objectMapper,
                                     SamDao samDao,
                                     ActivityLogger activityLogger) {
        this.recordDao = recordDao;
        this.batchWriteService = batchWriteService;
        this.recordService = recordService;
        this.instanceService = instanceService;
        this.objectMapper = objectMapper;
        this.samDao = samDao;
        this.activityLogger = activityLogger;
    }

    public RecordResponse updateSingleRecord(UUID instanceId, String version, RecordType recordType, String recordId,
                              RecordRequest recordRequest) {
        validateAndPermissions(instanceId, version);
        checkRecordTypeExists(instanceId, recordType);
        RecordResponse response = recordService.updateSingleRecord(instanceId, recordType, recordId, recordRequest);
        activityLogger.saveEventForCurrentUser(user ->
                user.updated().record().withRecordType(recordType).withId(recordId));
        return response;
    }

    public void validateAndPermissions(UUID instanceId, String version) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);

        boolean hasWriteInstancePermission = samDao.hasWriteInstancePermission();
        LOGGER.debug("hasWriteInstancePermission? {}", hasWriteInstancePermission);

        if (!hasWriteInstancePermission) {
            throw new AuthorizationException("Caller does not have permission to write to instance.");
        }
    }

    @ReadTransaction
    public RecordResponse getSingleRecord(UUID instanceId, String version, RecordType recordType, String recordId) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        Record result = recordDao.getSingleRecord(instanceId, recordType, recordId).orElseThrow(() -> new MissingObjectException("Record"));
        return new RecordResponse(recordId, recordType, result.getAttributes());
    }

    // N.B. transaction annotated in batchWriteService.batchWriteTsvStream
    public int tsvUpload(UUID instanceId, String version, RecordType recordType, Optional<String> primaryKey,
                          MultipartFile records) throws IOException {
        validateAndPermissions(instanceId, version);
        if(recordDao.recordTypeExists(instanceId, recordType)){
            recordService.validatePrimaryKey(instanceId, recordType, primaryKey);
        }
        int qty = batchWriteService.batchWriteTsvStream(records.getInputStream(), instanceId, recordType, primaryKey);
        activityLogger.saveEventForCurrentUser(user ->
                user.upserted().record().withRecordType(recordType).ofQuantity(qty));
        return qty;
    }

    // TODO: enable read transaction
    public StreamingResponseBody streamAllEntities(UUID instanceId, String version, RecordType recordType) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        List<String> headers = recordDao.getAllAttributeNames(instanceId, recordType);

        // TODO: consider rewriting this using jackson-dataformat-csv and removing org.apache.commons:commons-csv altogether
        return httpResponseOutputStream -> {
            try (Stream<Record> allRecords = recordDao.streamAllRecordsForType(instanceId, recordType);
                 CSVPrinter writer = TsvSupport.getOutputFormat(headers)
                     .print(new OutputStreamWriter(httpResponseOutputStream))) {
                TsvSupport.RecordEmitter recordEmitter = new TsvSupport.RecordEmitter(writer,
                    headers.subList(1, headers.size()), objectMapper);
                allRecords.forEach(recordEmitter);
            }
        };
    }

    @ReadTransaction
    public RecordQueryResponse queryForRecords(UUID instanceId, RecordType recordType, String version,
                                               SearchRequest searchRequest) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        if (null == searchRequest) {
            searchRequest = new SearchRequest();
        }
        if (searchRequest.getLimit() > MAX_RECORDS || searchRequest.getLimit() < 1 || searchRequest.getOffset() < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                "Limit must be more than 0 and can't exceed " + MAX_RECORDS + ", and offset must be positive.");
        }
        if (searchRequest.getSortAttribute() != null && !recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType)
                .containsKey(searchRequest.getSortAttribute())) {
            throw new MissingObjectException("Requested sort attribute");
        }
        int totalRecords = recordDao.countRecords(instanceId, recordType);
        if (searchRequest.getOffset() > totalRecords) {
            return new RecordQueryResponse(searchRequest, Collections.emptyList(), totalRecords);
        }
        LOGGER.info("queryForEntities: {}", recordType.getName());
        List<Record> records = recordDao.queryForRecords(recordType, searchRequest.getLimit(),
            searchRequest.getOffset(), searchRequest.getSort().name().toLowerCase(),
            searchRequest.getSortAttribute(), instanceId);
        List<RecordResponse> recordList = records.stream().map(
                r -> new RecordResponse(r.getId(), r.getRecordType(), r.getAttributes()))
            .toList();
        return new RecordQueryResponse(searchRequest, recordList, totalRecords);
    }

    public ResponseEntity<RecordResponse> upsertSingleRecord(UUID instanceId,String version, RecordType recordType,
                                                             String recordId, Optional<String> primaryKey,
                                                             RecordRequest recordRequest) {
        validateAndPermissions(instanceId, version);
        ResponseEntity<RecordResponse> response = recordService.upsertSingleRecord(instanceId, recordType, recordId, primaryKey, recordRequest);

        if (response.getStatusCode() == HttpStatus.CREATED) {
            activityLogger.saveEventForCurrentUser(user ->
                    user.created().record().withRecordType(recordType).withId(recordId));
        } else {
            activityLogger.saveEventForCurrentUser(user ->
                    user.updated().record().withRecordType(recordType).withId(recordId));
        }
        return response;

    }

    public boolean deleteSingleRecord(UUID instanceId, String version, RecordType recordType, String recordId) {
        validateAndPermissions(instanceId, version);
        checkRecordTypeExists(instanceId, recordType);
        boolean response = recordService.deleteSingleRecord(instanceId, recordType, recordId);
        activityLogger.saveEventForCurrentUser(user ->
                user.deleted().record().withRecordType(recordType).withId(recordId));
        return response;

    }

    public void deleteRecordType(UUID instanceId, String version, RecordType recordType) {
        validateAndPermissions(instanceId, version);
        checkRecordTypeExists(instanceId, recordType);
        recordService.deleteRecordType(instanceId, recordType);
        activityLogger.saveEventForCurrentUser(user ->
                user.deleted().table().ofQuantity(1).withRecordType(recordType));
    }

    @ReadTransaction
    public RecordTypeSchema describeRecordType(UUID instanceId, String version, RecordType recordType) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        return getSchemaDescription(instanceId, recordType);
    }

    @ReadTransaction
    public List<RecordTypeSchema> describeAllRecordTypes(UUID instanceId, String version) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        List<RecordType> allRecordTypes = recordDao.getAllRecordTypes(instanceId);
        return allRecordTypes.stream()
            .map(recordType -> getSchemaDescription(instanceId, recordType)).toList();
    }

    public int streamingWrite(UUID instanceId, String version, RecordType recordType,
                                                        Optional<String> primaryKey, InputStream is) {
        validateAndPermissions(instanceId, version);
        if(recordDao.recordTypeExists(instanceId, recordType)){
            recordService.validatePrimaryKey(instanceId, recordType, primaryKey);
        }

        int qty = batchWriteService.batchWriteJsonStream(is, instanceId, recordType, primaryKey);
        activityLogger.saveEventForCurrentUser(user ->
                user.modified().record().withRecordType(recordType).ofQuantity(qty));
        return qty;
    }

    private void checkRecordTypeExists(UUID instanceId, RecordType recordType) {
        if (!recordDao.recordTypeExists(instanceId, recordType)) {
            throw new MissingObjectException("Record type");
        }
    }

    private RecordTypeSchema getSchemaDescription(UUID instanceId, RecordType recordType) {
        Map<String, DataTypeMapping> schema = recordDao.getExistingTableSchema(instanceId, recordType);
        List<Relation> relationCols = recordDao.getRelationArrayCols(instanceId, recordType);
        relationCols.addAll(recordDao.getRelationCols(instanceId, recordType));
        Map<String, RecordType> relations = relationCols.stream()
            .collect(Collectors.toMap(Relation::relationColName, Relation::relationRecordType));
        List<AttributeSchema> attrSchema = schema.entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(entry -> new AttributeSchema(entry.getKey(), entry.getValue().toString(), relations.get(entry.getKey())))
            .toList();
        int recordCount = recordDao.countRecords(instanceId, recordType);
        return new RecordTypeSchema(recordType, attrSchema, recordCount, recordDao.getPrimaryKeyColumn(recordType, instanceId));
    }
}
