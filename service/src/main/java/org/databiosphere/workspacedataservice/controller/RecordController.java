package org.databiosphere.workspacedataservice.controller;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.StreamingWriteService;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

@RestController
public class RecordController {

	private static final int MAX_RECORDS = 1_000;
	private final RecordDao recordDao;
	private final DataTypeInferer inferer;

	private final StreamingWriteService writeService;

	public RecordController(RecordDao recordDao, StreamingWriteService writeService) {
		this.recordDao = recordDao;
		this.writeService = writeService;
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
		Map<String, Object> updatedAtts = recordRequest.recordAttributes().getAttributes();
		Map<String, Object> allAttrs = new HashMap<>(singleRecord.getAttributes().getAttributes());
		allAttrs.putAll(updatedAtts);

		Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(updatedAtts);
		Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchema(instanceId, recordType);
		singleRecord.setAttributes(new RecordAttributes(allAttrs));
		List<Record> records = Collections.singletonList(singleRecord);
		Map<String, DataTypeMapping> updatedSchema = addOrUpdateColumnIfNeeded(instanceId, recordType, typeMapping,
				existingTableSchema, records);
		recordDao.batchUpsert(instanceId, recordType, records, updatedSchema);
		RecordResponse response = new RecordResponse(recordId, recordType, singleRecord.getAttributes(),
				new RecordMetadata("TODO: SUPERFRESH"));
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	private Map<String, DataTypeMapping> addOrUpdateColumnIfNeeded(UUID instanceId, RecordType recordType,
			Map<String, DataTypeMapping> schema, Map<String, DataTypeMapping> existingTableSchema,
			List<Record> records) {
		MapDifference<String, DataTypeMapping> difference = Maps.difference(existingTableSchema, schema);
		Map<String, DataTypeMapping> colsToAdd = difference.entriesOnlyOnRight();
		colsToAdd.keySet().stream().filter(s -> s.startsWith(RESERVED_NAME_PREFIX)).findAny().ifPresent(s -> {
			throw new InvalidNameException(InvalidNameException.NameType.ATTRIBUTE);
		});
		validateRelationsAndAddColumns(instanceId, recordType, schema, records, colsToAdd, existingTableSchema);
		Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap = difference.entriesDiffering();
		for (Map.Entry<String, MapDifference.ValueDifference<DataTypeMapping>> entry : differenceMap.entrySet()) {
			String column = entry.getKey();
			MapDifference.ValueDifference<DataTypeMapping> valueDifference = entry.getValue();
			DataTypeMapping updatedColType = inferer.selectBestType(valueDifference.leftValue(),
					valueDifference.rightValue());
			recordDao.changeColumn(instanceId, recordType, column, updatedColType);
			schema.put(column, updatedColType);
		}
		return schema;
	}

	private void validateRelationsAndAddColumns(UUID instanceId, RecordType recordType,
			Map<String, DataTypeMapping> requestSchema, List<Record> records, Map<String, DataTypeMapping> colsToAdd,
			Map<String, DataTypeMapping> existingSchema) {
		Set<Relation> relations = RelationUtils.findRelations(records);
		List<Relation> existingRelations = recordDao.getRelationCols(instanceId, recordType);
		Set<String> existingRelationCols = existingRelations.stream().map(Relation::relationColName)
				.collect(Collectors.toSet());
		// look for case where requested relation column already exists as a
		// non-relational column
		for (Relation relation : relations) {
			String col = relation.relationColName();
			if (!existingRelationCols.contains(col) && existingSchema.containsKey(col)) {
				throw new InvalidRelationException("It looks like you're attempting to assign a relation "
						+ "to an existing attribute that was not configured for relations");
			}
		}
		relations.addAll(existingRelations);
		Map<String, List<Relation>> allRefCols = relations.stream()
				.collect(Collectors.groupingBy(Relation::relationColName));
		if (allRefCols.values().stream().anyMatch(l -> l.size() > 1)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Relation attribute can only be assigned to one record type");
		}
		for (Map.Entry<String, DataTypeMapping> entry : colsToAdd.entrySet()) {
			RecordType referencedRecordType = null;
			String col = entry.getKey();
			DataTypeMapping dataType = entry.getValue();
			if (allRefCols.containsKey(col)) {
				referencedRecordType = allRefCols.get(col).get(0).relationRecordType();
			}
			recordDao.addColumn(instanceId, recordType, col, dataType, referencedRecordType);
			requestSchema.put(col, dataType);
		}
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
		Map<String, Object> attributesInRequest = recordRequest.recordAttributes().getAttributes();
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
			existingTableSchema.keySet().forEach(attr -> attributesInRequest.putIfAbsent(attr, null));
			if (recordDao.recordExists(instanceId, recordType, recordId)) {
				status = HttpStatus.OK;
			}
			Record newRecord = new Record(recordId, recordType, recordRequest.recordAttributes());
			List<Record> records = Collections.singletonList(newRecord);
			addOrUpdateColumnIfNeeded(instanceId, recordType, requestSchema, existingTableSchema, records);
			Map<String, DataTypeMapping> combinedSchema = new HashMap<>(existingTableSchema);
			combinedSchema.putAll(requestSchema);
			recordDao.batchUpsert(instanceId, recordType, records, combinedSchema);
			RecordResponse response = new RecordResponse(recordId, recordType,
					new RecordAttributes(attributesInRequest), new RecordMetadata("TODO"));
			return new ResponseEntity<>(response, status);
		}
	}

	@PostMapping("/{instanceId}/{version}/")
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
	public ResponseEntity<Void> streamingWrite(@PathVariable("instanceid") UUID instanceId, @PathVariable("v") String version,
											   @PathVariable("type") RecordType recordType, InputStream is) {
		validateInstance(instanceId);
		validateVersion(version);
		writeService.consumeWriteStream(is, 500, instanceId, recordType);
		return ResponseEntity.ok().build();
	}


	@PostMapping("/{instanceid}/{v}/{type}/upload")
	public String handleStreamingUpload(@PathVariable("instanceid") UUID instanceId, @PathVariable("v") String version,
										@PathVariable("type") RecordType recordType, @RequestParam("file") MultipartFile file) throws IOException {
		validateInstance(instanceId);
		validateVersion(version);
		InputStreamReader inputStreamReader = new InputStreamReader(file.getInputStream());

		CSVFormat csvFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT).setHeader().build();
		CSVParser rows = csvFormat.parse(inputStreamReader);
		int batchSize = 10_000;
		boolean recordTypeExists = recordDao.recordTypeExists(instanceId, recordType);
		List<Record> batch = new ArrayList<>();
		Map<String, DataTypeMapping> schema = null;
		for (CSVRecord row : rows) {
			Map<String, Object> m = (Map)row.toMap();
			batch.add(new Record(row.get("id"), recordType, new RecordAttributes(m)));
			if(batch.size() >= batchSize) {
				if (!recordTypeExists) {
					schema = inferer.inferTypes(batch);
					recordDao.createRecordType(instanceId, schema, recordType, Collections.emptySet());
					recordTypeExists = true;
				}
				recordDao.batchUpsert(instanceId, recordType, batch, schema);
				batch.clear();
			}
		}
		if(!recordTypeExists){
			recordDao.createRecordType(instanceId, schema, recordType, Collections.emptySet());
		}
		recordDao.batchUpsert(instanceId, recordType, batch, inferer.inferTypes(batch));
		inputStreamReader.close();
		return "done";
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
