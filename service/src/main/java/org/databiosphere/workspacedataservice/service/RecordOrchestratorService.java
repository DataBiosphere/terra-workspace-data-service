package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.ReadTransaction;
import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.NewPrimaryKeyException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
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
import java.util.HashMap;
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
    private final DataTypeInferer inferer;
    private final BatchWriteService batchWriteService;
    private final RecordService recordService;
    private final InstanceService instanceService;
    private final ObjectMapper objectMapper;

    public RecordOrchestratorService(RecordDao recordDao,
                                     DataTypeInferer inferer,
                                     BatchWriteService batchWriteService,
                                     RecordService recordService,
                                     InstanceService instanceService,
                                     ObjectMapper objectMapper) {
        this.recordDao = recordDao;
        this.inferer = inferer;
        this.batchWriteService = batchWriteService;
        this.recordService = recordService;
        this.instanceService = instanceService;
        this.objectMapper = objectMapper;
    }

    @WriteTransaction
    public RecordResponse updateSingleRecord(UUID instanceId, String version, RecordType recordType, String recordId,
                              RecordRequest recordRequest) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        Record singleRecord = recordDao
            .getSingleRecord(instanceId, recordType, recordId)
            .orElseThrow(() -> new MissingObjectException("Record"));
        RecordAttributes incomingAtts = recordRequest.recordAttributes();
        RecordAttributes allAttrs = singleRecord.putAllAttributes(incomingAtts).getAttributes();
        Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(incomingAtts);
        Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType);
        singleRecord.setAttributes(allAttrs);
        List<Record> records = Collections.singletonList(singleRecord);
        Map<String, DataTypeMapping> updatedSchema = batchWriteService.addOrUpdateColumnIfNeeded(instanceId, recordType,
            typeMapping, existingTableSchema, records);
        recordService.prepareAndUpsert(instanceId, recordType, records, updatedSchema, recordDao.getPrimaryKeyColumn(recordType, instanceId));
        return new RecordResponse(recordId, recordType, singleRecord.getAttributes());
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
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        if(recordDao.recordTypeExists(instanceId, recordType)){
            validatePrimaryKey(instanceId, recordType, primaryKey);
        }
        return batchWriteService.batchWriteTsvStream(records.getInputStream(), instanceId, recordType, primaryKey);
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
            .keySet().contains(searchRequest.getSortAttribute())) {
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

    @WriteTransaction
    public ResponseEntity<RecordResponse> upsertSingleRecord(UUID instanceId,String version, RecordType recordType,
                                                             String recordId, Optional<String> primaryKey,
                                                             RecordRequest recordRequest) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        RecordAttributes attributesInRequest = recordRequest.recordAttributes();
        Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest);
        HttpStatus status = HttpStatus.CREATED;
        if (!recordDao.recordTypeExists(instanceId, recordType)) {
            RecordResponse response = new RecordResponse(recordId, recordType, recordRequest.recordAttributes());
            Record newRecord = new Record(recordId, recordType, recordRequest);
            createRecordTypeAndInsertRecords(instanceId, newRecord, recordType, requestSchema, primaryKey);
            return new ResponseEntity<>(response, status);
        } else {
            validatePrimaryKey(instanceId, recordType, primaryKey);
            Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType);
            // null out any attributes that already exist but aren't in the request
            existingTableSchema.keySet().forEach(attr -> attributesInRequest.putAttributeIfAbsent(attr, null));
            if (recordDao.recordExists(instanceId, recordType, recordId)) {
                status = HttpStatus.OK;
            }
            Record newRecord = new Record(recordId, recordType, recordRequest.recordAttributes());
            List<Record> records = Collections.singletonList(newRecord);
            batchWriteService.addOrUpdateColumnIfNeeded(instanceId, recordType, requestSchema, existingTableSchema,
                records);
            Map<String, DataTypeMapping> combinedSchema = new HashMap<>(existingTableSchema);
            combinedSchema.putAll(requestSchema);
            recordService.prepareAndUpsert(instanceId, recordType, records, combinedSchema, primaryKey.orElseGet(() -> recordDao.getPrimaryKeyColumn(recordType, instanceId)));
            RecordResponse response = new RecordResponse(recordId, recordType, attributesInRequest);
            return new ResponseEntity<>(response, status);
        }
    }

    @WriteTransaction
    public boolean deleteSingleRecord(UUID instanceId, String version, RecordType recordType, String recordId) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        return recordDao.deleteSingleRecord(instanceId, recordType, recordId);
    }

    @WriteTransaction
    public void deleteRecordType(UUID instanceId, String version, RecordType recordType) {
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        checkRecordTypeExists(instanceId, recordType);
        recordDao.deleteRecordType(instanceId, recordType);
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
        validateVersion(version);
        instanceService.validateInstance(instanceId);
        if(recordDao.recordTypeExists(instanceId, recordType)){
            validatePrimaryKey(instanceId, recordType, primaryKey);
        }
        return batchWriteService.batchWriteJsonStream(is, instanceId, recordType, primaryKey);
    }

    private void validatePrimaryKey(UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
        if (primaryKey.isPresent() && !primaryKey.get().equals(recordDao.getPrimaryKeyColumn(recordType, instanceId))) {
            throw new NewPrimaryKeyException(primaryKey.get(), recordType);
        }
    }

    private void checkRecordTypeExists(UUID instanceId, RecordType recordType) {
        if (!recordDao.recordTypeExists(instanceId, recordType)) {
            throw new MissingObjectException("Record type");
        }
    }

    private void createRecordTypeAndInsertRecords(UUID instanceId, Record newRecord, RecordType recordType,
                                                  Map<String, DataTypeMapping> requestSchema,
                                                  Optional<String> primaryKey) {
        List<Record> records = Collections.singletonList(newRecord);
        recordDao.createRecordType(instanceId, requestSchema, recordType, inferer.findRelations(records, requestSchema), primaryKey.orElse(ReservedNames.RECORD_ID));
        recordService.prepareAndUpsert(instanceId, recordType, records, requestSchema, primaryKey.orElse(ReservedNames.RECORD_ID));
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
