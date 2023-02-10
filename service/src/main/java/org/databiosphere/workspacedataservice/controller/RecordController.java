package org.databiosphere.workspacedataservice.controller;

import bio.terra.common.db.ReadTransaction;
import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.*;
import org.databiosphere.workspacedataservice.service.model.*;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.NewPrimaryKeyException;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
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
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class RecordController {

	private final RecordDao recordDao;
	private final RecordOrchestratorService recordOrchestratorService;

	public RecordController(RecordDao recordDao, RecordOrchestratorService recordOrchestratorService) {
		this.recordDao = recordDao;
		this.recordOrchestratorService = recordOrchestratorService;
	}

	@PatchMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@WriteTransaction
	public ResponseEntity<RecordResponse> updateSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestBody RecordRequest recordRequest) {
		RecordResponse response = recordOrchestratorService
			.updateSingleRecord(instanceId, version, recordType, recordId, recordRequest);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@ReadTransaction
	public ResponseEntity<RecordResponse> getSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		RecordResponse response = recordOrchestratorService.getSingleRecord(instanceId, version, recordType, recordId);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping( "/{instanceId}/tsv/{version}/{recordType}")
	// N.B. transaction annotated in batchWriteService.uploadTsvStream
	public ResponseEntity<TsvUploadResponse> tsvUpload(@PathVariable("instanceId") UUID instanceId,
			   @PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			   @RequestParam(name= "primaryKey", required = false) Optional<String> primaryKey,
               @RequestParam("records") MultipartFile records) throws IOException {
		int recordsModified = recordOrchestratorService.tsvUpload(instanceId, version, recordType, primaryKey, records);
		return new ResponseEntity<>(new TsvUploadResponse(recordsModified, "Updated " + recordType.toString()),
				HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/tsv/{version}/{recordType}")
	// TODO: enable read transaction
	public ResponseEntity<StreamingResponseBody> streamAllEntities(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType) {
		StreamingResponseBody responseBody =
			recordOrchestratorService.streamAllEntities(instanceId, version, recordType);
		return ResponseEntity.status(HttpStatus.OK).contentType(new MediaType("text", "tab-separated-values"))
				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + recordType.getName() + ".tsv")
				.body(responseBody);
	}

	@PostMapping("/{instanceid}/search/{version}/{recordType}")
	@ReadTransaction
	public RecordQueryResponse queryForRecords(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("recordType") RecordType recordType,
			@PathVariable("version") String version,
			@RequestBody(required = false) SearchRequest searchRequest) {
		return recordOrchestratorService.queryForRecords(instanceId, recordType, version, searchRequest);
	}

	@PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@WriteTransaction
	public ResponseEntity<RecordResponse> upsertSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestParam(name= "primaryKey", required = false) Optional<String> primaryKey,
			 @RequestBody RecordRequest recordRequest) {
			return recordOrchestratorService.upsertSingleRecord(instanceId, version, recordType, recordId, primaryKey,
				recordRequest);
	}

	private void validatePrimaryKey(UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		if (primaryKey.isPresent() && !primaryKey.get().equals(recordDao.getPrimaryKeyColumn(recordType, instanceId))) {
			throw new NewPrimaryKeyException(primaryKey.get(), recordType);
		}
	}

	@GetMapping("/instances/{version}")
	@ReadTransaction
	public ResponseEntity<List<UUID>> listInstances(@PathVariable("version") String version) {
		RecordOrchestratorService.validateVersion(version);
		List<UUID> schemaList = recordDao.listInstanceSchemas();
		return new ResponseEntity<>(schemaList, HttpStatus.OK);
	}

	@PostMapping("/instances/{version}/{instanceId}")
	@WriteTransaction
	public ResponseEntity<String> createInstance(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version) {
		RecordOrchestratorService.validateVersion(version);
		if (recordDao.instanceSchemaExists(instanceId)) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "This instance already exists");
		}
		recordDao.createSchema(instanceId);
		return new ResponseEntity<>(HttpStatus.CREATED);
	}

	@DeleteMapping("/instances/{version}/{instanceId}")
	@WriteTransaction
	public ResponseEntity<String> deleteInstance(@PathVariable("instanceId") UUID instanceId,
												 @PathVariable("version") String version) {
		RecordOrchestratorService.validateVersion(version);
		recordOrchestratorService.validateInstance(instanceId);
		recordDao.dropSchema(instanceId);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@WriteTransaction
	public ResponseEntity<Void> deleteSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		boolean recordFound = recordOrchestratorService.deleteSingleRecord(instanceId, version, recordType, recordId);
		return recordFound ? new ResponseEntity<>(HttpStatus.NO_CONTENT) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
	}

	@DeleteMapping("/{instanceId}/types/{v}/{type}")
	@WriteTransaction
	public ResponseEntity<Void> deleteRecordType(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType) {
		recordOrchestratorService.deleteRecordType(instanceId, version, recordType);
		return new ResponseEntity<>(HttpStatus.NO_CONTENT);
	}

	@GetMapping("/{instanceId}/types/{v}/{type}")
	@ReadTransaction
	public ResponseEntity<RecordTypeSchema> describeRecordType(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType) {
		RecordTypeSchema result = recordOrchestratorService.describeRecordType(instanceId, version, recordType);
		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/types/{v}")
	@ReadTransaction
	public ResponseEntity<List<RecordTypeSchema>> describeAllRecordTypes(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version) {
		List<RecordTypeSchema> result = recordOrchestratorService.describeAllRecordTypes(instanceId, version);
		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@PostMapping("/{instanceid}/batch/{v}/{type}")
	// N.B. transaction annotated in batchWriteService.consumeWriteStream
	public ResponseEntity<BatchResponse> streamingWrite(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType,
			@RequestParam(name= "primaryKey", required = false) Optional<String> primaryKey, InputStream is) {
		int recordsModified = recordOrchestratorService.streamingWrite(instanceId, version, recordType, primaryKey, is);
		return new ResponseEntity<>(new BatchResponse(recordsModified, "Huzzah"), HttpStatus.OK);
	}
}
