package org.databiosphere.workspacedataservice.controller;

import org.apache.commons.csv.CSVPrinter;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.TsvSupport;
import org.databiosphere.workspacedataservice.service.model.*;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class RecordController {

	private static final int MAX_RECORDS = 1_000;
	private final RecordDao recordDao;
	private final DataTypeInferer inferer;
	private final BatchWriteService batchWriteService;

	public RecordController(RecordDao recordDao, BatchWriteService batchWriteService) {
		this.recordDao = recordDao;
		this.batchWriteService = batchWriteService;
		this.inferer = new DataTypeInferer();
	}

	@PatchMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> updateSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		checkRecordTypeExists(instanceId, recordType);
		Record singleRecord = recordDao
				.getSingleRecord(instanceId, recordType, recordId, recordDao.getRelationCols(instanceId, recordType))
				.orElseThrow(() -> new MissingObjectException("Record"));
		RecordAttributes incomingAtts = recordRequest.recordAttributes();
		RecordAttributes allAttrs = singleRecord.putAllAttributes(incomingAtts).getAttributes();
		Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(incomingAtts);
		Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchema(instanceId, recordType);
		singleRecord.setAttributes(allAttrs);
		List<Record> records = Collections.singletonList(singleRecord);
		Map<String, DataTypeMapping> updatedSchema = batchWriteService.addOrUpdateColumnIfNeeded(instanceId, recordType,
				typeMapping, existingTableSchema, records);
		recordDao.batchUpsert(instanceId, recordType, records, updatedSchema);
		RecordResponse response = new RecordResponse(recordId, recordType, singleRecord.getAttributes(),
				new RecordMetadata("TODO: SUPERFRESH"));
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> getSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		Record result = recordDao
				.getSingleRecord(instanceId, recordType, recordId, recordDao.getRelationCols(instanceId, recordType))
				.orElseThrow(() -> new MissingObjectException("Record"));
		RecordResponse response = new RecordResponse(recordId, recordType, result.getAttributes(),
				new RecordMetadata("TODO: RECORDMETADATA"));
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@GetMapping("{instanceId}/tsv/{version}/{recordType}")
	public ResponseEntity<StreamingResponseBody> streamAllEntities(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType) {
		validateVersion(version);
		validateInstance(instanceId);
		checkRecordTypeExists(instanceId, recordType);
		List<String> headers = new ArrayList<>(Collections.singletonList(ReservedNames.RECORD_ID));
		headers.addAll(recordDao.getExistingTableSchema(instanceId, recordType).keySet());
		Stream<Record> allRecords = recordDao.streamAllRecordsForType(instanceId, recordType);

		StreamingResponseBody responseBody = httpResponseOutputStream -> {
			try (CSVPrinter writer = TsvSupport.getOutputFormat(headers)
					.print(new OutputStreamWriter(httpResponseOutputStream))) {
				TsvSupport.RecordEmitter recordEmitter = new TsvSupport.RecordEmitter(writer,
						headers.subList(1, headers.size()));
				allRecords.forEach(recordEmitter);
			}
		};
		return ResponseEntity.status(HttpStatus.OK).contentType(new MediaType("text", "tab-separated-values"))
				.body(responseBody);
	}

	@PostMapping("/{instanceid}/search/{version}/{recordType}")
	public RecordQueryResponse queryForEntities(@PathVariable("instanceid") UUID instanceId,
			@PathVariable("recordType") RecordType recordType,
			@RequestBody(required = false) SearchRequest searchRequest) {
		checkRecordTypeExists(instanceId, recordType);
		if (null == searchRequest) {
			searchRequest = new SearchRequest();
		}
		if (searchRequest.getLimit() > MAX_RECORDS || searchRequest.getLimit() < 1 || searchRequest.getOffset() < 0) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Limit must be more than 0 and can't exceed " + MAX_RECORDS + ", and offset must be positive.");
		}
		int totalRecords = recordDao.countRecords(instanceId, recordType);
		if (searchRequest.getOffset() > totalRecords) {
			return new RecordQueryResponse(searchRequest, Collections.emptyList(), totalRecords);
		}
		List<Record> records = recordDao.queryForRecords(recordType, searchRequest.getLimit(),
				searchRequest.getOffset(), searchRequest.getSort().name().toLowerCase(), instanceId);
		List<RecordResponse> recordList = records.stream().map(
				r -> new RecordResponse(r.getId(), r.getRecordType(), r.getAttributes(), new RecordMetadata("UNUSED")))
				.toList();
		return new RecordQueryResponse(searchRequest, recordList, totalRecords);
	}

	@PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> upsertSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		RecordAttributes attributesInRequest = recordRequest.recordAttributes();
		Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest);
		HttpStatus status = HttpStatus.CREATED;
		if (!recordDao.instanceSchemaExists(instanceId)) {
			recordDao.createSchema(instanceId);
		}
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			RecordResponse response = new RecordResponse(recordId, recordType, recordRequest.recordAttributes(),
					new RecordMetadata("TODO"));
			Record newRecord = new Record(recordId, recordType, recordRequest);
			createRecordTypeAndInsertRecords(instanceId, newRecord, recordType, requestSchema);
			return new ResponseEntity<>(response, status);
		} else {
			Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchema(instanceId, recordType);
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
			recordDao.batchUpsert(instanceId, recordType, records, combinedSchema);
			RecordResponse response = new RecordResponse(recordId, recordType, attributesInRequest,
					new RecordMetadata("TODO"));
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

	@DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<Void> deleteSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") String recordId) {
		validateVersion(version);
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
		Map<String, DataTypeMapping> schema = recordDao.getExistingTableSchema(instanceId, recordType);
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
		recordDao.createRecordType(instanceId, requestSchema, recordType, RelationUtils.findRelations(records));
		recordDao.batchUpsert(instanceId, recordType, records, requestSchema);
	}
}
