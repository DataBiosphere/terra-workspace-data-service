package org.databiosphere.workspacedataservice.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
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
import org.springframework.lang.Nullable;
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
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@DataPlane
@RestController
public class RecordController {

  private final CollectionService collectionService;
  private final RecordOrchestratorService recordOrchestratorService;

  public RecordController(
      CollectionService collectionService, RecordOrchestratorService recordOrchestratorService) {
    this.collectionService = collectionService;
    this.recordOrchestratorService = recordOrchestratorService;
  }

  @PatchMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
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
  public ResponseEntity<RecordResponse> getSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") String recordId) {
    RecordResponse response =
        recordOrchestratorService.getSingleRecord(instanceId, version, recordType, recordId);
    return new ResponseEntity<>(response, HttpStatus.OK);
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
        new TsvUploadResponse(recordsModified, "Updated " + recordType), HttpStatus.OK);
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
  public RecordQueryResponse queryForRecords(
      @PathVariable("instanceid") UUID instanceId,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("version") String version,
      @Nullable @RequestBody(required = false) SearchRequest searchRequest) {
    return recordOrchestratorService.queryForRecords(
        instanceId, recordType, version, searchRequest);
  }

  @PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
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
  public ResponseEntity<List<UUID>> listInstances(@PathVariable("version") String version) {
    List<UUID> schemaList = collectionService.listCollections(version);
    return new ResponseEntity<>(schemaList, HttpStatus.OK);
  }

  @PostMapping("/instances/{version}/{instanceId}")
  public ResponseEntity<String> createInstance(
      @PathVariable("instanceId") UUID instanceId, @PathVariable("version") String version) {
    collectionService.createCollection(instanceId, version);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  @DeleteMapping("/instances/{version}/{instanceId}")
  public ResponseEntity<String> deleteInstance(
      @PathVariable("instanceId") UUID instanceId, @PathVariable("version") String version) {
    collectionService.deleteCollection(instanceId, version);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
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
  public ResponseEntity<Void> deleteRecordType(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType) {
    recordOrchestratorService.deleteRecordType(instanceId, version, recordType);
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @PatchMapping("{instanceId}/types/{v}/{type}/{attribute}")
  public ResponseEntity<AttributeSchema> updateAttribute(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType,
      @PathVariable("attribute") String attribute,
      @RequestBody AttributeSchema newAttributeSchema) {
    Optional<String> optionalNewAttributeName = Optional.ofNullable(newAttributeSchema.name());
    Optional<String> optionalNewDataType = Optional.ofNullable(newAttributeSchema.datatype());

    if (optionalNewAttributeName.isEmpty() && optionalNewDataType.isEmpty()) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "At least one of name or datatype is required");
    }

    optionalNewAttributeName.ifPresent(
        newAttributeName ->
            recordOrchestratorService.renameAttribute(
                instanceId, version, recordType, attribute, newAttributeName));

    String finalAttributeName = optionalNewAttributeName.orElse(attribute);

    optionalNewDataType.ifPresent(
        newDataType ->
            recordOrchestratorService.updateAttributeDataType(
                instanceId, version, recordType, finalAttributeName, newDataType));

    RecordTypeSchema recordTypeSchema =
        recordOrchestratorService.describeRecordType(instanceId, version, recordType);
    AttributeSchema attributeSchema = recordTypeSchema.getAttributeSchema(finalAttributeName);
    return new ResponseEntity<>(attributeSchema, HttpStatus.OK);
  }

  @DeleteMapping("{instanceId}/types/{v}/{type}/{attribute}")
  public ResponseEntity<Void> deleteAttribute(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType,
      @PathVariable("attribute") String attribute) {
    recordOrchestratorService.deleteAttribute(instanceId, version, recordType, attribute);
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @GetMapping("/{instanceId}/types/{v}/{type}")
  public ResponseEntity<RecordTypeSchema> describeRecordType(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("v") String version,
      @PathVariable("type") RecordType recordType) {
    RecordTypeSchema result =
        recordOrchestratorService.describeRecordType(instanceId, version, recordType);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }

  @GetMapping("/{instanceId}/types/{v}")
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
