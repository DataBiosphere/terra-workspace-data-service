package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;

import bio.terra.common.db.ReadTransaction;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.recordsink.RecordSink;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.PrimaryKeyResolver;
import org.databiosphere.workspacedataservice.recordsource.RecordSource;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.recordsource.TsvRecordSource;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
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
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Service
public class RecordOrchestratorService { // TODO give me a better name

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordOrchestratorService.class);
  private static final int MAX_RECORDS = 1_000;

  private final RecordDao recordDao;
  private final RecordSourceFactory recordSourceFactory;
  private final RecordSinkFactory recordSinkFactory;
  private final BatchWriteService batchWriteService;
  private final RecordService recordService;
  private final CollectionService collectionService;
  private final ActivityLogger activityLogger;

  private final TsvSupport tsvSupport;

  public RecordOrchestratorService(
      RecordDao recordDao,
      RecordSourceFactory recordSourceFactory,
      RecordSinkFactory recordSinkFactory,
      BatchWriteService batchWriteService,
      RecordService recordService,
      CollectionService collectionService,
      ActivityLogger activityLogger,
      TsvSupport tsvSupport) {
    this.recordDao = recordDao;
    this.recordSourceFactory = recordSourceFactory;
    this.recordSinkFactory = recordSinkFactory;
    this.batchWriteService = batchWriteService;
    this.recordService = recordService;
    this.collectionService = collectionService;
    this.activityLogger = activityLogger;
    this.tsvSupport = tsvSupport;
  }

  public RecordResponse updateSingleRecord(
      UUID collectionId,
      String version,
      RecordType recordType,
      String recordId,
      RecordRequest recordRequest) {
    validateAndPermissions(collectionId, version);
    checkRecordTypeExists(collectionId, recordType);
    RecordResponse response =
        recordService.updateSingleRecord(collectionId, recordType, recordId, recordRequest);
    activityLogger.saveEventForCurrentUser(
        user -> user.updated().record().withRecordType(recordType).withId(recordId));
    return response;
  }

  public void validateAndPermissions(UUID collectionId, String version) {
    validateVersion(version);
    collectionService.validateCollection(collectionId);

    // check that the caller has write permissions on the workspace associated with the collectionId
    boolean hasWriteWorkspacePermission =
        collectionService.canWriteCollection(CollectionId.of(collectionId));
    LOGGER.debug("hasWriteWorkspacePermission? {}", hasWriteWorkspacePermission);

    if (!hasWriteWorkspacePermission) {
      throw new AuthorizationException("Caller does not have permission to write to collection.");
    }
  }

  @ReadTransaction
  public RecordResponse getSingleRecord(
      UUID collectionId, String version, RecordType recordType, String recordId) {
    validateVersion(version);
    collectionService.validateCollection(collectionId);
    checkRecordTypeExists(collectionId, recordType);
    Record result =
        recordDao
            .getSingleRecord(collectionId, recordType, recordId)
            .orElseThrow(() -> new MissingObjectException("Record"));
    return new RecordResponse(recordId, recordType, result.getAttributes());
  }

  // N.B. transaction annotated in batchWriteService.batchWrite
  public int tsvUpload(
      UUID collectionId,
      String version,
      RecordType recordType,
      Optional<String> primaryKey,
      MultipartFile records)
      throws IOException, DataImportException {
    validateAndPermissions(collectionId, version);
    if (recordDao.recordTypeExists(collectionId, recordType)) {
      primaryKey =
          Optional.of(recordService.validatePrimaryKey(collectionId, recordType, primaryKey));
    }

    TsvRecordSource recordSource =
        recordSourceFactory.forTsv(records.getInputStream(), recordType, primaryKey);
    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(CollectionId.of(collectionId))) {
      BatchWriteResult result =
          batchWriteService.batchWrite(
              recordSource,
              recordSink,
              recordType,
              // the extra cast here isn't exactly necessary, but left here to call out the
              // additional tangential responsibility of the TsvRecordSource; this can be removed if
              // we can converge on using PrimaryKeyResolver more generally across all formats.
              ((PrimaryKeyResolver) recordSource).getPrimaryKey());
      int qty = result.getUpdatedCount(recordType);
      activityLogger.saveEventForCurrentUser(
          user -> user.upserted().record().withRecordType(recordType).ofQuantity(qty));
      return qty;
    }
  }

  // TODO: enable read transaction
  public StreamingResponseBody streamAllEntities(
      UUID collectionId, String version, RecordType recordType) {
    validateVersion(version);
    collectionService.validateCollection(collectionId);
    checkRecordTypeExists(collectionId, recordType);
    List<String> headers = recordDao.getAllAttributeNames(collectionId, recordType);

    Map<String, DataTypeMapping> typeSchema =
        recordDao.getExistingTableSchema(collectionId, recordType);

    return httpResponseOutputStream -> {
      try (Stream<Record> allRecords =
          recordDao.streamAllRecordsForType(collectionId, recordType)) {
        tsvSupport.writeTsvToStream(allRecords, typeSchema, httpResponseOutputStream, headers);
      }
    };
  }

  @ReadTransaction
  public RecordQueryResponse queryForRecords(
      UUID collectionId,
      RecordType recordType,
      String version,
      // SearchRequest isn't required in the controller, so it can be null here
      @Nullable SearchRequest searchRequest) {
    validateVersion(version);
    collectionService.validateCollection(collectionId);
    checkRecordTypeExists(collectionId, recordType);
    if (null == searchRequest) {
      searchRequest = new SearchRequest();
    }
    if (searchRequest.getLimit() > MAX_RECORDS
        || searchRequest.getLimit() < 1
        || searchRequest.getOffset() < 0) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "Limit must be more than 0 and can't exceed "
              + MAX_RECORDS
              + ", and offset must be positive.");
    }
    if (searchRequest.getSortAttribute() != null
        && !recordDao
            .getExistingTableSchema(collectionId, recordType)
            .containsKey(searchRequest.getSortAttribute())) {
      throw new MissingObjectException("Requested sort attribute");
    }
    int totalRecords = recordDao.countRecords(collectionId, recordType);
    if (searchRequest.getOffset() > totalRecords) {
      return new RecordQueryResponse(searchRequest, Collections.emptyList(), totalRecords);
    }
    LOGGER.info("queryForEntities: {}", recordType.getName());
    List<Record> records =
        recordDao.queryForRecords(
            recordType,
            searchRequest.getLimit(),
            searchRequest.getOffset(),
            searchRequest.getSort().name().toLowerCase(),
            searchRequest.getSortAttribute(),
            searchRequest.getFilter(),
            collectionId);
    List<RecordResponse> recordList =
        records.stream()
            .map(r -> new RecordResponse(r.getId(), r.getRecordType(), r.getAttributes()))
            .toList();
    return new RecordQueryResponse(searchRequest, recordList, totalRecords);
  }

  public ResponseEntity<RecordResponse> upsertSingleRecord(
      UUID collectionId,
      String version,
      RecordType recordType,
      String recordId,
      Optional<String> primaryKey,
      RecordRequest recordRequest) {
    validateAndPermissions(collectionId, version);
    ResponseEntity<RecordResponse> response =
        recordService.upsertSingleRecord(
            collectionId, recordType, recordId, primaryKey, recordRequest);

    if (response.getStatusCode() == HttpStatus.CREATED) {
      activityLogger.saveEventForCurrentUser(
          user -> user.created().record().withRecordType(recordType).withId(recordId));
    } else {
      activityLogger.saveEventForCurrentUser(
          user -> user.updated().record().withRecordType(recordType).withId(recordId));
    }
    return response;
  }

  public boolean deleteSingleRecord(
      UUID collectionId, String version, RecordType recordType, String recordId) {
    validateAndPermissions(collectionId, version);
    checkRecordTypeExists(collectionId, recordType);
    boolean response = recordService.deleteSingleRecord(collectionId, recordType, recordId);
    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().record().withRecordType(recordType).withId(recordId));
    return response;
  }

  public void deleteRecordType(UUID collectionId, String version, RecordType recordType) {
    validateAndPermissions(collectionId, version);
    checkRecordTypeExists(collectionId, recordType);
    recordService.deleteRecordType(collectionId, recordType);
    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().table().ofQuantity(1).withRecordType(recordType));
  }

  public void renameAttribute(
      UUID collectionId,
      String version,
      RecordType recordType,
      String attribute,
      String newAttributeName) {
    validateAndPermissions(collectionId, version);
    checkRecordTypeExists(collectionId, recordType);
    validateRenameAttribute(collectionId, recordType, attribute, newAttributeName);
    recordService.renameAttribute(collectionId, recordType, attribute, newAttributeName);
    activityLogger.saveEventForCurrentUser(
        user ->
            user.renamed()
                .attribute()
                .withRecordType(recordType)
                .withIds(new String[] {attribute, newAttributeName}));
  }

  private void validateRenameAttribute(
      UUID collectionId, RecordType recordType, String attribute, String newAttributeName) {
    RecordTypeSchema schema = getSchemaDescription(collectionId, recordType);

    if (schema.isPrimaryKey(attribute)) {
      throw new ValidationException("Unable to rename primary key attribute");
    }
    if (!schema.containsAttribute(attribute)) {
      throw new MissingObjectException("Attribute");
    }
    if (schema.containsAttribute(newAttributeName)) {
      throw new ConflictException("Attribute already exists");
    }
  }

  public void updateAttributeDataType(
      UUID collectionId,
      String version,
      RecordType recordType,
      String attribute,
      String newDataType) {
    validateAndPermissions(collectionId, version);
    checkRecordTypeExists(collectionId, recordType);
    RecordTypeSchema schema = getSchemaDescription(collectionId, recordType);
    if (schema.isPrimaryKey(attribute)) {
      throw new ValidationException("Unable to update primary key attribute");
    }

    DataTypeMapping newDataTypeMapping = validateAttributeDataType(newDataType);
    try {
      recordService.updateAttributeDataType(
          collectionId, recordType, attribute, newDataTypeMapping);
    } catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage());
    }
    activityLogger.saveEventForCurrentUser(
        user -> user.updated().attribute().withRecordType(recordType).withId(attribute));
  }

  private DataTypeMapping validateAttributeDataType(String dataType) {
    try {
      return DataTypeMapping.valueOf(dataType);
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid datatype");
    }
  }

  public void deleteAttribute(
      UUID collectionId, String version, RecordType recordType, String attribute) {
    validateAndPermissions(collectionId, version);
    checkRecordTypeExists(collectionId, recordType);
    validateDeleteAttribute(collectionId, recordType, attribute);
    recordService.deleteAttribute(collectionId, recordType, attribute);
    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().attribute().withRecordType(recordType).withId(attribute));
  }

  private void validateDeleteAttribute(UUID collectionId, RecordType recordType, String attribute) {
    RecordTypeSchema schema = getSchemaDescription(collectionId, recordType);

    if (schema.isPrimaryKey(attribute)) {
      throw new ValidationException("Unable to delete primary key attribute");
    }
    if (!schema.containsAttribute(attribute)) {
      throw new MissingObjectException("Attribute");
    }
  }

  @ReadTransaction
  public RecordTypeSchema describeRecordType(
      UUID collectionId, String version, RecordType recordType) {
    validateVersion(version);
    collectionService.validateCollection(collectionId);
    checkRecordTypeExists(collectionId, recordType);
    return getSchemaDescription(collectionId, recordType);
  }

  @ReadTransaction
  public List<RecordTypeSchema> describeAllRecordTypes(UUID collectionId, String version) {
    validateVersion(version);
    collectionService.validateCollection(collectionId);
    List<RecordType> allRecordTypes = recordDao.getAllRecordTypes(collectionId);
    return allRecordTypes.stream()
        .map(recordType -> getSchemaDescription(collectionId, recordType))
        .toList();
  }

  public int streamingWrite(
      UUID collectionId,
      String version,
      RecordType recordType,
      Optional<String> primaryKey,
      InputStream is)
      throws DataImportException {
    validateAndPermissions(collectionId, version);
    if (recordDao.recordTypeExists(collectionId, recordType)) {
      recordService.validatePrimaryKey(collectionId, recordType, primaryKey);
    }

    RecordSource recordSource;
    try {
      recordSource = recordSourceFactory.forJson(is);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }

    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(CollectionId.of(collectionId))) {
      BatchWriteResult result =
          batchWriteService.batchWrite(
              recordSource, recordSink, recordType, primaryKey.orElse(RECORD_ID));
      int qty = result.getUpdatedCount(recordType);
      activityLogger.saveEventForCurrentUser(
          user -> user.modified().record().withRecordType(recordType).ofQuantity(qty));
      return qty;
    }
  }

  private void checkRecordTypeExists(UUID collectionId, RecordType recordType) {
    if (!recordDao.recordTypeExists(collectionId, recordType)) {
      throw new MissingObjectException("Record type");
    }
  }

  private RecordTypeSchema getSchemaDescription(UUID collectionId, RecordType recordType) {
    Map<String, DataTypeMapping> schema =
        recordDao.getExistingTableSchema(collectionId, recordType);
    List<Relation> relationCols = recordDao.getRelationArrayCols(collectionId, recordType);
    relationCols.addAll(recordDao.getRelationCols(collectionId, recordType));
    Map<String, RecordType> relations =
        relationCols.stream()
            .collect(Collectors.toMap(Relation::relationColName, Relation::relationRecordType));
    List<AttributeSchema> attrSchema =
        schema.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(
                entry ->
                    new AttributeSchema(
                        entry.getKey(), entry.getValue().toString(), relations.get(entry.getKey())))
            .toList();
    int recordCount = recordDao.countRecords(collectionId, recordType);
    return new RecordTypeSchema(
        recordType,
        attrSchema,
        recordCount,
        recordDao.getPrimaryKeyColumn(recordType, collectionId));
  }
}
