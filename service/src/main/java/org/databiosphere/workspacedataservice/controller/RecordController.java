package org.databiosphere.workspacedataservice.controller;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordQueryResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchRequest;
import org.databiosphere.workspacedataservice.shared.model.TsvUploadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  // all otel stuff
  // depending on what we want to track, we will have less or more of such
  // at a minimum each class that tracks Otel stuff will need the client passed in
  private final OpenTelemetry openTelemetry;

  // we will always need a tracer
  private final Tracer tracer;

  // this is for a specific metric that tracks how often something happens
  // i.e. how many times a given api call is made
  private final LongCounter recordApiInvocations;

  // this is for a specific metric that in this case will track how long it takes for
  // an api call to complete. Historgram format allows for specifics graphs to be made
  // in whatever system is used downstream
  private final DoubleHistogram histogram;

  // attributes that will be used inside the otel logs and metrics
  private static final AttributeKey<String> ATTR_N = AttributeKey.stringKey("http.recordTypeName");
  private static final AttributeKey<String> ATTR_RESULT = AttributeKey.stringKey("http.result");
  private static final AttributeKey<Boolean> ATTR_VALID_N =
      AttributeKey.booleanKey("record.getSingleRecord");
  private static final AttributeKey<String> ATTR_APICALL = AttributeKey.stringKey("http.apicall");

  public RecordController(
      InstanceService instanceService,
      RecordOrchestratorService recordOrchestratorService,
      OpenTelemetry openTelemetry) {
    this.instanceService = instanceService;
    this.recordOrchestratorService = recordOrchestratorService;
    this.openTelemetry = openTelemetry;
    this.tracer = openTelemetry.getTracer(Controller.class.getName());

    // these are specific to metrics (different from spans/traces)
    Meter meter = openTelemetry.getMeter(Controller.class.getName());
    recordApiInvocations =
        meter
            .counterBuilder("record.invocations")
            .setDescription("Measures the number of times the getSingleRecord api is invoked.")
            .build();

    histogram =
        meter
            .histogramBuilder("record.requestTime")
            .setDescription("Measures how long a given records request takes. ")
            .setUnit("ms")
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
    // define span for this scope using main tracer
    var startTime = Timestamp.from(Instant.now());
    var span =
        tracer
            // set span scope (tracer has been scoped to this class)
            .spanBuilder("getSingleRecord")
            // alternatively you can also set a span attribute to capture which api call this is
            // likely there is a better way to capture this not in a string that repeats a few times
            .setAttribute(ATTR_APICALL, "getSingleRecord")
            // set an attribute for the name of the record
            .setAttribute(ATTR_N, recordType.getName())
            // other attributes that we could set could be: instance Id, version, etc
            .startSpan();

    // basic events can also be added if desired
    span.addEvent("getSingleRecord is called. ");

    try (var scope = span.makeCurrent()) {
      // in here to experiment with how duration is being recorded
      Thread.sleep(5000);
      RecordResponse response =
          recordOrchestratorService.getSingleRecord(instanceId, version, recordType, recordId);

      // Counter to increment when a valid input is recorded
      recordApiInvocations.add(1, Attributes.of(ATTR_VALID_N, true));

      // Set a span attribute to capture information about successful requests
      // response status code not available here since it is being set somewhere downstream :/
      // this captures number of successful calls
      span.setAttribute(ATTR_RESULT, HttpStatus.OK.toString());

      var endTime = Timestamp.from(Instant.now());
      // add duration of api (in this example calculated manually)
      // I spent a significant amount of time trying to figure out how to not do this manually
      // what I learned is:
      // span by default has a start and end time, but end time is not recorded until span is closed
      // from what I can tell there is no easy way to get span start or end directly in code
      // if it was, then I would not need to have a timer tracking how long something takes
      histogram.record(
          endTime.getTime() - startTime.getTime(),
          Attributes.builder().put("ATTR_APICALL", "getSingleRecord").build());
      return new ResponseEntity<>(response, HttpStatus.OK);
    } catch (MissingObjectException e) {
      // Record the exception and set the span status
      // this captures number of error calls
      // althought currently it is a bit more tricky - since spring can set a response
      // based on a type of exception and if we do not capture this specific exception in the catch
      // here the error otel event will not be captured
      // we would only do this if we wanted to capture specific http status vs just success/fail
      span.setAttribute(ATTR_RESULT, HttpStatus.BAD_REQUEST.toString());
      span.recordException(e).setStatus(StatusCode.ERROR, e.getMessage());
      // Counter to increment when an invalid input is recorded
      recordApiInvocations.add(1, Attributes.of(ATTR_VALID_N, false));
      throw e;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      // End the span
      span.end();

      // if one could figure out how to get start and end of span, metric for how long api takes
      // can be captured here
      // histogram.record(...)
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
