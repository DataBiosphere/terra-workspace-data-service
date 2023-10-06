package org.databiosphere.workspacedataservice.controller;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.databiosphere.workspacedataservice.shared.model.TsvUploadResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.Controller;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
public class RecordController {

  private final InstanceService instanceService;
  private final RecordOrchestratorService recordOrchestratorService;

  private final OpenTelemetry openTelemetry;
  private final Tracer tracer;
  private final TextMapPropagator textMapPropagator;

  private final LongCounter recordApiInvocations;

  private static final AttributeKey<String> ATTR_N = AttributeKey.stringKey("http.record");
  private static final AttributeKey<Long> ATTR_RESULT = AttributeKey.longKey("http.result");

  public RecordController(
      InstanceService instanceService,
      RecordOrchestratorService recordOrchestratorService,
      OpenTelemetry openTelemetry) {
    this.instanceService = instanceService;
    this.recordOrchestratorService = recordOrchestratorService;
    this.openTelemetry = openTelemetry;
    this.tracer = openTelemetry.getTracer(Controller.class.getName());
    this.textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();
    Meter meter = openTelemetry.getMeter(Controller.class.getName());
    recordApiInvocations =
        meter
            .counterBuilder("record.invocations")
            .setDescription("Measures the number of times the record api is invoked.")
            .build();
  }

  @PatchMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
  @RetryableApi
  public ResponseEntity<RecordResponse> updateSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") String recordId,
      @RequestBody RecordRequest recordRequest) {
    RecordResponse response =
        recordOrchestratorService.updateSingleRecord(
            instanceId, version, recordType, recordId, recordRequest);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
  @RetryableApi
  public ResponseEntity<RecordResponse> getSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") String recordId) {
    var span =
        tracer
            .spanBuilder("RecordController")
            .setAttribute(ATTR_N, recordType.getName())
            .startSpan();
    try (var scope = span.makeCurrent()) {
      RecordResponse response =
          recordOrchestratorService.getSingleRecord(instanceId, version, recordType, recordId);
      span.setAttribute(ATTR_RESULT, HttpStatus.OK.value());
      return new ResponseEntity<>(response, HttpStatus.OK response);
    } catch (Exception e) {
      span.recordException(e).setStatus(StatusCode.ERROR, e.getMessage());
      throw e;
    } finally {
      // End the span
      span.end();
    }
  }

  @PostMapping("/{instanceId}/tsv/{version}/{recordType}")
  public ResponseEntity<TsvUploadResponse> tsvUpload(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @RequestParam(name = "primaryKey", required = false) Optional<String> primaryKey,
      @RequestParam("records") MultipartFile records)
      throws IOException {
    int recordsModified =
        recordOrchestratorService.tsvUpload(instanceId, version, recordType, primaryKey, records);
    return new ResponseEntity<>(
        new TsvUploadResponse(recordsModified, "Updated " + recordType.toString()), HttpStatus.OK);
  }

  @GetMapping("/{instanceId}/tsv/{version}/{recordType}")
  public ResponseEntity<StreamingResponseBody> streamAllEntities(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType) {
    StreamingResponseBody responseBody =
        recordOrchestratorService.streamAllEntities(instanceId, version, recordType);
    return ResponseEntity.status(HttpStatus.OK)
        .contentType(new MediaType("text", "tab-separated-values"))
        .header(
            HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + recordType.getName() + ".tsv")
        .body(responseBody);
  }

  @PostMapping("/{instanceid}/search/{version}/{recordType}")
  @RetryableApi
  public RecordQueryResponse queryForRecords(
      @PathVariable("instanceid") UUID instanceId,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("version") String version,
      @RequestBody(required = false) SearchRequest searchRequest) {
    return recordOrchestratorService.queryForRecords(
        instanceId, recordType, version, searchRequest);
  }

  @PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
  @RetryableApi
  public ResponseEntity<RecordResponse> upsertSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") String recordId,
      @RequestParam(name = "primaryKey", required = false) Optional<String> primaryKey,
      @RequestBody RecordRequest recordRequest) {
    return recordOrchestratorService.upsertSingleRecord(
        instanceId, version, recordType, recordId, primaryKey, recordRequest);
  }

  @GetMapping("/instances/{version}")
  @RetryableApi
  public ResponseEntity<List<UUID>> listInstances(@PathVariable("version") String version) {
    List<UUID> schemaList = instanceService.listInstances(version);
    return new ResponseEntity<>(schemaList, HttpStatus.OK);
  }

  @PostMapping("/instances/{version}/{instanceId}")
  public ResponseEntity<String> createInstance(
      @PathVariable("instanceId") UUID instanceId, @PathVariable("version") String version) {
    instanceService.createInstance(instanceId, version);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  @DeleteMapping("/instances/{version}/{instanceId}")
  public ResponseEntity<String> deleteInstance(
      @PathVariable("instanceId") UUID instanceId, @PathVariable("version") String version) {
    instanceService.deleteInstance(instanceId, version);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
  @RetryableApi
  public ResponseEntity<Void> deleteSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") String recordId) {
    boolean recordFound =
        recordOrchestratorService.deleteSingleRecord(instanceId, version, recordType, recordId);
    return recordFound
        ? new ResponseEntity<>(HttpStatus.NO_CONTENT)
        : new ResponseEntity<>(HttpStatus.NOT_FOUND);
  }

  @DeleteMapping("/{instanceId}/types/{v}/{type}")
  @RetryableApi
  public ResponseEntity<Void> deleteRecordType(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType) {
    recordOrchestratorService.deleteRecordType(instanceId, version, recordType);
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @GetMapping("/{instanceId}/types/{v}/{type}")
  @RetryableApi
  public ResponseEntity<RecordTypeSchema> describeRecordType(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType) {
    RecordTypeSchema result =
        recordOrchestratorService.describeRecordType(instanceId, version, recordType);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }

  @GetMapping("/{instanceId}/types/{v}")
  @RetryableApi
  public ResponseEntity<List<RecordTypeSchema>> describeAllRecordTypes(
      @PathVariable("instanceId") UUID instanceId, @PathVariable("v") String version) {
    List<RecordTypeSchema> result =
        recordOrchestratorService.describeAllRecordTypes(instanceId, version);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }

  @PostMapping("/{instanceid}/batch/{v}/{type}")
  // N.B. transaction annotated in batchWriteService.batchWriteJsonStream
  public ResponseEntity<BatchResponse> streamingWrite(
      @PathVariable("instanceid") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType,
      @RequestParam(name = "primaryKey", required = false) Optional<String> primaryKey,
      InputStream is) {
    int recordsModified =
        recordOrchestratorService.streamingWrite(instanceId, version, recordType, primaryKey, is);
    return new ResponseEntity<>(new BatchResponse(recordsModified, "Huzzah"), HttpStatus.OK);
  }
}
