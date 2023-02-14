package org.databiosphere.workspacedataservice.controller;

import bio.terra.common.db.ReadTransaction;
import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.TsvSupport;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.ReservedNames;
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
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class RecordController {

	private static final Logger LOGGER = LoggerFactory.getLogger(RecordController.class);
	private static final int MAX_RECORDS = 1_000;
	private final RecordDao recordDao;
	private final DataTypeInferer inferer;
	private final BatchWriteService batchWriteService;
	private final RecordService recordService;

	private final ObjectMapper objectMapper;

	public RecordController(RecordDao recordDao, BatchWriteService batchWriteService, DataTypeInferer inf,
			ObjectMapper objectMapper, RecordService recordService) {
		this.recordDao = recordDao;
		this.batchWriteService = batchWriteService;
		this.inferer = inf;
		this.objectMapper = objectMapper;
		this.recordService = recordService;
	}

	@PatchMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@WriteTransaction
	public ResponseEntity<RecordResponse> updateSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		validateInstance(instanceId);
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
		RecordResponse response = new RecordResponse(recordId, recordType, singleRecord.getAttributes());
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@ReadTransaction
	public ResponseEntity<RecordResponse> getSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		Record result = recordDao.getSingleRecord(instanceId, recordType, recordId).orElseThrow(() -> new MissingObjectException("Record"));
		RecordResponse response = new RecordResponse(recordId, recordType, result.getAttributes());
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping( "/{instanceId}/tsv/{version}/{recordType}")
	// N.B. transaction annotated in batchWriteService.batchWriteTsvStream
	public ResponseEntity<TsvUploadResponse> tsvUpload(@PathVariable("instanceId") UUID instanceId,
			   @PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			   @RequestParam(name= "primaryKey", required = false) Optional<String> primaryKey,
               @RequestParam("records") MultipartFile records) throws IOException {
		validateVersion(version);
		validateInstance(instanceId);
		if(recordDao.recordTypeExists(instanceId, recordType)){
			validatePrimaryKey(instanceId, recordType, primaryKey);
		}
		int recordsModified = batchWriteService.batchWriteTsvStream(records.getInputStream(), instanceId, recordType, primaryKey);
		return new ResponseEntity<>(new TsvUploadResponse(recordsModified, "Updated " + recordType.toString()),
				HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/tsv/{version}/{recordType}")
	// TODO: enable read transaction
	public ResponseEntity<StreamingResponseBody> streamAllEntities(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		List<String> headers = recordDao.getAllAttributeNames(instanceId, recordType);

		// TODO: consider rewriting this using jackson-dataformat-csv and removing org.apache.commons:commons-csv altogether
		StreamingResponseBody responseBody = httpResponseOutputStream -> {
			try (Stream<Record> allRecords = recordDao.streamAllRecordsForType(instanceId, recordType);
				 CSVPrinter writer = TsvSupport.getOutputFormat(headers)
					.print(new OutputStreamWriter(httpResponseOutputStream))) {
				TsvSupport.RecordEmitter recordEmitter = new TsvSupport.RecordEmitter(writer,
						headers.subList(1, headers.size()), objectMapper);
				allRecords.forEach(recordEmitter);
			}
		};
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
		validateVersion(version);
		validateInstance(instanceId);
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

	@PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@WriteTransaction
	public ResponseEntity<RecordResponse> upsertSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestParam(name= "primaryKey", required = false) Optional<String> primaryKey,
			 @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		validateInstance(instanceId);
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

	private void validatePrimaryKey(UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		if (primaryKey.isPresent() && !primaryKey.get().equals(recordDao.getPrimaryKeyColumn(recordType, instanceId))) {
			throw new NewPrimaryKeyException(primaryKey.get(), recordType);
		}
	}

	@GetMapping("/instances/{version}")
	@ReadTransaction
	public ResponseEntity<List<UUID>> listInstances(@PathVariable("version") String version) {
		validateVersion(version);
		List<UUID> schemaList = recordDao.listInstanceSchemas();
		return new ResponseEntity<>(schemaList, HttpStatus.OK);
	}


	@PostMapping("/instances/{version}/{instanceId}")
	@WriteTransaction
	public ResponseEntity<String> createInstance(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version) {
		validateVersion(version);
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
		validateVersion(version);
		validateInstance(instanceId);
		recordDao.dropSchema(instanceId);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	@WriteTransaction
	public ResponseEntity<Void> deleteSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		boolean recordFound = recordDao.deleteSingleRecord(instanceId, recordType, recordId);
		return recordFound ? new ResponseEntity<>(HttpStatus.NO_CONTENT) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
	}

	@DeleteMapping("/{instanceId}/types/{v}/{type}")
	@WriteTransaction
	public ResponseEntity<Void> deleteRecordType(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		recordDao.deleteRecordType(instanceId, recordType);
		return new ResponseEntity<>(HttpStatus.NO_CONTENT);
	}

	@GetMapping("/{instanceId}/types/{v}/{type}")
	@ReadTransaction
	public ResponseEntity<RecordTypeSchema> describeRecordType(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		RecordTypeSchema result = getSchemaDescription(instanceId, recordType);
		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/types/{v}")
	@ReadTransaction
	public ResponseEntity<List<RecordTypeSchema>> describeAllRecordTypes(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version) {
		validateVersion(version);
		validateInstance(instanceId);
		List<RecordType> allRecordTypes = recordDao.getAllRecordTypes(instanceId);
		List<RecordTypeSchema> result = allRecordTypes.stream()
				.map(recordType -> getSchemaDescription(instanceId, recordType)).toList();
		return new ResponseEntity<>(result, HttpStatus.OK);
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

	private static void validateVersion(String version) {
		if (null == version || !version.equals("v0.2")) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid API version specified");
		}
	}

	private void checkRecordTypeExists(UUID instanceId, RecordType recordType) {
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			throw new MissingObjectException("Record type");
		}
	}

	@PostMapping("/{instanceid}/batch/{v}/{type}")
	// N.B. transaction annotated in batchWriteService.batchWriteJsonStream
	public ResponseEntity<BatchResponse> streamingWrite(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType,
			@RequestParam(name= "primaryKey", required = false) Optional<String> primaryKey, InputStream is) {
		validateVersion(version);
		validateInstance(instanceId);
		if(recordDao.recordTypeExists(instanceId, recordType)){
			validatePrimaryKey(instanceId, recordType, primaryKey);
		}
		int recordsModified = batchWriteService.batchWriteJsonStream(is, instanceId, recordType, primaryKey);
		return new ResponseEntity<>(new BatchResponse(recordsModified, "Huzzah"), HttpStatus.OK);
	}

	private void validateInstance(UUID instanceId) {
		if (!recordDao.instanceSchemaExists(instanceId)) {
			throw new MissingObjectException("Instance");
		}
	}

	private void createRecordTypeAndInsertRecords(UUID instanceId, Record newRecord, RecordType recordType,
		Map<String, DataTypeMapping> requestSchema, Optional<String> primaryKey) {
		List<Record> records = Collections.singletonList(newRecord);
		recordDao.createRecordType(instanceId, requestSchema, recordType, inferer.findRelations(records, requestSchema), primaryKey.orElse(ReservedNames.RECORD_ID));
		recordService.prepareAndUpsert(instanceId, recordType, records, requestSchema, primaryKey.orElse(ReservedNames.RECORD_ID));
	}

}
