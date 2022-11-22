package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.*;
import org.databiosphere.workspacedataservice.service.model.*;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
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
		Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(incomingAtts, InBoundDataSource.JSON);
		Map<String, DataTypeMapping> existingTableSchema = getFullTableSchema(instanceId, recordType);
		singleRecord.setAttributes(allAttrs);
		List<Record> records = Collections.singletonList(singleRecord);
		Map<String, DataTypeMapping> updatedSchema = batchWriteService.addOrUpdateColumnIfNeeded(instanceId, recordType,
				typeMapping, existingTableSchema, records);
		recordService.prepareAndUpsert(instanceId, recordType, records, updatedSchema);
		RecordResponse response = new RecordResponse(recordId, recordType, singleRecord.getAttributes());
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> getSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		Record result = getSingleRecord(instanceId, recordType, recordId);
		RecordResponse response = new RecordResponse(recordId, recordType, result.getAttributes());
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	private Record getSingleRecord(UUID instanceId, RecordType recordType, String recordId){
		Record result = recordDao
				.getSingleRecord(instanceId, recordType, recordId)
				.orElseThrow(() -> new MissingObjectException("Record"));
		List<Relation> relationArrays = recordDao.getRelationArrayCols(instanceId, recordType);
		if (relationArrays.isEmpty()){
			return result;
		}
		for (Relation rel : relationArrays){
			List<String> relArr = recordDao.getRelationArrayValues(instanceId, rel, result);
			result.setAttributeValue(rel.relationColName(), relArr);
		}
		return result;
	}

	@PostMapping( "/{instanceId}/tsv/{version}/{recordType}")
	public ResponseEntity<TsvUploadResponse> tsvUpload(@PathVariable("instanceId") UUID instanceId,
			   @PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			   @RequestParam(name= "uniqueRowIdentifierColumn", required = false) Optional<String> uniqueRowIdentifierColumn,
               @RequestParam("records") MultipartFile records) throws IOException {
		validateVersion(version);
		validateInstance(instanceId);
		int recordsModified;
		try (InputStreamReader inputStreamReader = new InputStreamReader(records.getInputStream())) {
			recordsModified = batchWriteService.uploadTsvStream(inputStreamReader, instanceId, recordType, uniqueRowIdentifierColumn);
		}
		return new ResponseEntity<>(new TsvUploadResponse(recordsModified, "Updated " + recordType.toString()),
				HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/tsv/{version}/{recordType}")
	public ResponseEntity<StreamingResponseBody> streamAllEntities(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		List<String> headers = recordDao.getAllAttributeNames(instanceId, recordType);
		Stream<Record> allRecords = recordDao.streamAllRecordsForType(instanceId, recordType);

		StreamingResponseBody responseBody = httpResponseOutputStream -> {
			try (CSVPrinter writer = TsvSupport.getOutputFormat(headers)
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
	public RecordQueryResponse queryForEntities(@PathVariable("instanceid") UUID instanceId,
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
		if (searchRequest.getSortAttribute() != null && !getFullTableSchema(instanceId, recordType)
				.keySet().contains(searchRequest.getSortAttribute())) {
			throw new MissingObjectException("Requested sort attribute");
		}
		int totalRecords = recordDao.countRecords(instanceId, recordType);
		if (searchRequest.getOffset() > totalRecords) {
			return new RecordQueryResponse(searchRequest, Collections.emptyList(), totalRecords);
		}
		LOGGER.info("queryForEntities: {}", recordType.getName());
		List<Record> records = queryForRecords(recordType, searchRequest.getLimit(),
				searchRequest.getOffset(), searchRequest.getSort().name().toLowerCase(),
				searchRequest.getSortAttribute(), instanceId);
		List<RecordResponse> recordList = records.stream().map(
				r -> new RecordResponse(r.getId(), r.getRecordType(), r.getAttributes()))
				.toList();
		return new RecordQueryResponse(searchRequest, recordList, totalRecords);
	}

	private List<Record> queryForRecords(RecordType recordType, int pageSize, int offset, String sortDirection,
										 String sortAttribute, UUID instanceId){
		List<Relation> relationArrays = recordDao.getRelationArrayCols(instanceId, recordType);
		//TODO: Sort by relation array attribute
		//For now, defaulting to sorting by primary key if sort attribute is a relation array column
		if (relationArrays.stream().map(Relation::relationColName).toList().contains(sortAttribute)){
			sortAttribute = null;
		}
		List<Record> records = recordDao.queryForRecords(recordType, pageSize,
				offset, sortDirection, sortAttribute, instanceId);
		for (Record rec : records){
			for (Relation rel : relationArrays){
				List<String> relArr = recordDao.getRelationArrayValues(instanceId, rel, rec);
				rec.setAttributeValue(rel.relationColName(), relArr);
			}
		}
		return records;
	}

	@PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> upsertSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		validateInstance(instanceId);
		RecordAttributes attributesInRequest = recordRequest.recordAttributes();
		Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest, InBoundDataSource.JSON);
		HttpStatus status = HttpStatus.CREATED;
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			RecordResponse response = new RecordResponse(recordId, recordType, recordRequest.recordAttributes());
			Record newRecord = new Record(recordId, recordType, recordRequest);
			createRecordTypeAndInsertRecords(instanceId, newRecord, recordType, requestSchema);
			return new ResponseEntity<>(response, status);
		} else {
			Map<String, DataTypeMapping> existingTableSchema = getFullTableSchema(instanceId, recordType);
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
			recordService.prepareAndUpsert(instanceId, recordType, records, combinedSchema);
			RecordResponse response = new RecordResponse(recordId, recordType, attributesInRequest);
			return new ResponseEntity<>(response, status);
		}
	}

	@PostMapping("/instances/{version}/{instanceId}")
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
	public ResponseEntity<String> deleteInstance(@PathVariable("instanceId") UUID instanceId,
												 @PathVariable("version") String version) {
		validateVersion(version);
		validateInstance(instanceId);
		recordDao.dropSchema(instanceId);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
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
	public ResponseEntity<Void> deleteRecordType(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		recordDao.deleteRecordType(instanceId, recordType);
		return new ResponseEntity<>(HttpStatus.NO_CONTENT);
	}

	@GetMapping("/{instanceId}/types/{v}/{type}")
	public ResponseEntity<RecordTypeSchema> describeRecordType(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		RecordTypeSchema result = getSchemaDescription(instanceId, recordType);
		return new ResponseEntity<>(result, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/types/{v}")
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
		Map<String, DataTypeMapping> schema = getFullTableSchema(instanceId, recordType);
		Map<String, RecordType> relations = recordDao.getRelationCols(instanceId, recordType).stream()
				.collect(Collectors.toMap(Relation::relationColName, Relation::relationRecordType));
		List<AttributeSchema> attrSchema = schema.entrySet().stream().sorted(Map.Entry.comparingByKey())
				.map(entry -> createAttributeSchema(entry.getKey(), entry.getValue(), relations.get(entry.getKey())))
				.toList();
		int recordCount = recordDao.countRecords(instanceId, recordType);
		return new RecordTypeSchema(recordType, attrSchema, recordCount);
	}

	private AttributeSchema createAttributeSchema(String name, DataTypeMapping datatype, RecordType relation) {
		if (relation == null) {
			return new AttributeSchema(name, datatype.toString(), null);
		}
		return new AttributeSchema(name, "RELATION", relation);
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
	public ResponseEntity<BatchResponse> streamingWrite(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("v") String version, @PathVariable("type") RecordType recordType, InputStream is) {
		validateVersion(version);
		validateInstance(instanceId);
		int recordsModified = batchWriteService.consumeWriteStream(is, instanceId, recordType);
		return new ResponseEntity<>(new BatchResponse(recordsModified, "Huzzah"), HttpStatus.OK);
	}

	private void validateInstance(UUID instanceId) {
		if (!recordDao.instanceSchemaExists(instanceId)) {
			throw new MissingObjectException("Instance");
		}
	}

	private void createRecordTypeAndInsertRecords(UUID instanceId, Record newRecord, RecordType recordType,
			Map<String, DataTypeMapping> requestSchema) {
		List<Record> records = Collections.singletonList(newRecord);
		recordDao.createRecordType(instanceId, requestSchema, recordType, RelationUtils.findRelations(records), ReservedNames.RECORD_ID);
		recordService.prepareAndUpsert(instanceId, recordType, records, requestSchema);
	}

	private Map<String, DataTypeMapping> getFullTableSchema(UUID instanceId, RecordType recordType){
		List<Relation> relationArrays = recordDao.getRelationArrayCols(instanceId, recordType);
		Map<String, DataTypeMapping> schema = recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType);
		for (Relation rel : relationArrays){
			schema.put(rel.relationColName(), DataTypeMapping.ARRAY_OF_RELATION);
		}
		return schema;
	}

}
