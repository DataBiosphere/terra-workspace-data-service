package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.validateVersion;

import bio.terra.common.db.ReadTransaction;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.TextMapPropagator;
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
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.NewPrimaryKeyException;
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
import org.springframework.web.servlet.mvc.Controller;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Service
public class RecordOrchestratorService { // TODO give me a better name

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordOrchestratorService.class);
  private static final int MAX_RECORDS = 1_000;

  private final RecordDao recordDao;
  private final BatchWriteService batchWriteService;
  private final RecordService recordService;
  private final InstanceService instanceService;
  private final SamDao samDao;
  private final ActivityLogger activityLogger;

  private final TsvSupport tsvSupport;

  private final OpenTelemetry openTelemetry;
  private final Tracer tracer;
  private final TextMapPropagator textMapPropagator;

  private final LongCounter recordApiInvocations;

  private final DoubleHistogram histogram;

  private static final AttributeKey<String> ATTR_N = AttributeKey.stringKey("http.recordTypeName");
  private static final AttributeKey<String> ATTR_RESULT = AttributeKey.stringKey("http.result");
  private static final AttributeKey<String> ATTR_APICALL = AttributeKey.stringKey("http.apicall");
  private static final AttributeKey<Boolean> ATTR_VALID_N =
      AttributeKey.booleanKey("record.upsertSingleRecord");

  public RecordOrchestratorService(
      RecordDao recordDao,
      BatchWriteService batchWriteService,
      RecordService recordService,
      InstanceService instanceService,
      SamDao samDao,
      ActivityLogger activityLogger,
      TsvSupport tsvSupport,
      OpenTelemetry openTelemetry) {
    this.recordDao = recordDao;
    this.batchWriteService = batchWriteService;
    this.recordService = recordService;
    this.instanceService = instanceService;
    this.samDao = samDao;
    this.activityLogger = activityLogger;
    this.tsvSupport = tsvSupport;
    this.openTelemetry = openTelemetry;
    this.tracer = openTelemetry.getTracer(Controller.class.getName());
    this.textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();
    Meter meter = openTelemetry.getMeter(Controller.class.getName());
    recordApiInvocations =
        meter
            .counterBuilder("record.invocations_service")
            .setDescription("Measures the number of times an api is invoked.")
            .build();

    histogram =
        meter
            .histogramBuilder("record.requestTime_service")
            .setDescription("Measures how long a given records api request takes. ")
            .setUnit("ms")
            .build();
  }

  public RecordResponse updateSingleRecord(
      UUID instanceId,
      String version,
      RecordType recordType,
      String recordId,
      RecordRequest recordRequest) {
    validateAndPermissions(instanceId, version);
    checkRecordTypeExists(instanceId, recordType);
    RecordResponse response =
        recordService.updateSingleRecord(instanceId, recordType, recordId, recordRequest);
    activityLogger.saveEventForCurrentUser(
        user -> user.updated().record().withRecordType(recordType).withId(recordId));
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
  public RecordResponse getSingleRecord(
      UUID instanceId, String version, RecordType recordType, String recordId) {
    validateVersion(version);
    instanceService.validateInstance(instanceId);
    checkRecordTypeExists(instanceId, recordType);
    Record result =
        recordDao
            .getSingleRecord(instanceId, recordType, recordId)
            .orElseThrow(() -> new MissingObjectException("Record"));
    return new RecordResponse(recordId, recordType, result.getAttributes());
  }

  // N.B. transaction annotated in batchWriteService.batchWriteTsvStream
  public int tsvUpload(
      UUID instanceId,
      String version,
      RecordType recordType,
      Optional<String> primaryKey,
      MultipartFile records)
      throws IOException {
    validateAndPermissions(instanceId, version);
    if (recordDao.recordTypeExists(instanceId, recordType)) {
      primaryKey =
          Optional.of(recordService.validatePrimaryKey(instanceId, recordType, primaryKey));
    }
    int qty =
        batchWriteService.batchWriteTsvStream(
            records.getInputStream(), instanceId, recordType, primaryKey);
    activityLogger.saveEventForCurrentUser(
        user -> user.upserted().record().withRecordType(recordType).ofQuantity(qty));
    return qty;
  }

  // TODO: enable read transaction
  public StreamingResponseBody streamAllEntities(
      UUID instanceId, String version, RecordType recordType) {
    validateVersion(version);
    instanceService.validateInstance(instanceId);
    checkRecordTypeExists(instanceId, recordType);
    List<String> headers = recordDao.getAllAttributeNames(instanceId, recordType);

    Map<String, DataTypeMapping> typeSchema =
        recordDao.getExistingTableSchema(instanceId, recordType);

    return httpResponseOutputStream -> {
      try (Stream<Record> allRecords = recordDao.streamAllRecordsForType(instanceId, recordType)) {
        tsvSupport.writeTsvToStream(allRecords, typeSchema, httpResponseOutputStream, headers);
      }
    };
  }

  @ReadTransaction
  public RecordQueryResponse queryForRecords(
      UUID instanceId, RecordType recordType, String version, SearchRequest searchRequest) {
    validateVersion(version);
    instanceService.validateInstance(instanceId);
    checkRecordTypeExists(instanceId, recordType);
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
            .getExistingTableSchemaLessPrimaryKey(instanceId, recordType)
            .containsKey(searchRequest.getSortAttribute())) {
      throw new MissingObjectException("Requested sort attribute");
    }
    int totalRecords = recordDao.countRecords(instanceId, recordType);
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
            instanceId);
    List<RecordResponse> recordList =
        records.stream()
            .map(r -> new RecordResponse(r.getId(), r.getRecordType(), r.getAttributes()))
            .toList();
    return new RecordQueryResponse(searchRequest, recordList, totalRecords);
  }

  public ResponseEntity<RecordResponse> upsertSingleRecord(
      UUID instanceId,
      String version,
      RecordType recordType,
      String recordId,
      Optional<String> primaryKey,
      RecordRequest recordRequest) {
    // may make sense to create this at the class level vs each method
    var span =
        tracer
            .spanBuilder("RecordController-upsertSingleRecord")
            .setAttribute(ATTR_N, recordType.getName())
            .startSpan();
    // Set a span attribute to capture which api call this is
    span.setAttribute(ATTR_APICALL, "upsertSingleRecord");
    span.addEvent("upsertSingleRecord is called. ");

    try (var scope = span.makeCurrent()) {
      validateAndPermissions(instanceId, version);

      ResponseEntity<RecordResponse> response =
          recordService.upsertSingleRecord(
              instanceId, recordType, recordId, primaryKey, recordRequest);

      // Set a span attribute to capture information about successful requests
      span.setAttribute(ATTR_RESULT, response.getStatusCode().toString());

      // Counter to increment when a valid input is recorded
      recordApiInvocations.add(1, Attributes.of(ATTR_VALID_N, true));

      // add duration of api
      // histogram.record(10.2, Attributes.builder().put("key", "value").build());

      if (response.getStatusCode() == HttpStatus.CREATED) {
        activityLogger.saveEventForCurrentUser(
            user -> user.created().record().withRecordType(recordType).withId(recordId));
      } else {
        activityLogger.saveEventForCurrentUser(
            user -> user.updated().record().withRecordType(recordType).withId(recordId));
      }

      return response;
    } catch (NewPrimaryKeyException e) {

      // Set a span attribute to capture information about error requests
      // currently the error response code does get captured but it is deep somewhere in the code
      span.setAttribute(ATTR_RESULT, HttpStatus.BAD_REQUEST.toString());
      throw e;
    } catch (Exception e) {
      LOGGER.info(
          "HELLLLLLLLLLLLLLLLLLLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo"
              + e.toString());
      // Record the exception and set the span status
      span.recordException(e).setStatus(StatusCode.ERROR, e.getMessage());

      // Counter to increment when an invalid input is recorded
      recordApiInvocations.add(1, Attributes.of(ATTR_VALID_N, false));

      throw e;
    } finally {
      // End the span
      span.end();

      // histogram.record(span.getSpanContext().);
    }
  }

  public boolean deleteSingleRecord(
      UUID instanceId, String version, RecordType recordType, String recordId) {
    validateAndPermissions(instanceId, version);
    checkRecordTypeExists(instanceId, recordType);
    boolean response = recordService.deleteSingleRecord(instanceId, recordType, recordId);
    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().record().withRecordType(recordType).withId(recordId));
    return response;
  }

  public void deleteRecordType(UUID instanceId, String version, RecordType recordType) {
    validateAndPermissions(instanceId, version);
    checkRecordTypeExists(instanceId, recordType);
    recordService.deleteRecordType(instanceId, recordType);
    activityLogger.saveEventForCurrentUser(
        user -> user.deleted().table().ofQuantity(1).withRecordType(recordType));
  }

  @ReadTransaction
  public RecordTypeSchema describeRecordType(
      UUID instanceId, String version, RecordType recordType) {
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
        .map(recordType -> getSchemaDescription(instanceId, recordType))
        .toList();
  }

  public int streamingWrite(
      UUID instanceId,
      String version,
      RecordType recordType,
      Optional<String> primaryKey,
      InputStream is) {
    validateAndPermissions(instanceId, version);
    if (recordDao.recordTypeExists(instanceId, recordType)) {
      recordService.validatePrimaryKey(instanceId, recordType, primaryKey);
    }

    int qty = batchWriteService.batchWriteJsonStream(is, instanceId, recordType, primaryKey);
    activityLogger.saveEventForCurrentUser(
        user -> user.modified().record().withRecordType(recordType).ofQuantity(qty));
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
    int recordCount = recordDao.countRecords(instanceId, recordType);
    return new RecordTypeSchema(
        recordType, attrSchema, recordCount, recordDao.getPrimaryKeyColumn(recordType, instanceId));
  }
}
