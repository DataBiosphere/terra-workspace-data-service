package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

@RestController
public class RecordController {

	private final RecordDao recordDao;
	private final DataTypeInferer inferer;

	public RecordController(RecordDao recordDao) {
		this.recordDao = recordDao;
		this.inferer = new DataTypeInferer();
	}

	@PatchMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> updateSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		String recordTypeName = recordType.getName();
		Record singleRecord = recordDao
				.getSingleRecord(instanceId, recordType, recordId,
						recordDao.getRelationCols(instanceId, recordTypeName))
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Record not found"));
		Map<String, Object> updatedAtts = recordRequest.recordAttributes().getAttributes();
		Map<String, Object> allAttrs = new HashMap<>(singleRecord.getAttributes().getAttributes());
		allAttrs.putAll(updatedAtts);

		Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(updatedAtts);
		Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchema(instanceId, recordTypeName);
		singleRecord.setAttributes(new RecordAttributes(allAttrs));
		List<Record> records = Collections.singletonList(singleRecord);
		Map<String, DataTypeMapping> updatedSchema = addOrUpdateColumnIfNeeded(instanceId, recordType.getName(),
				typeMapping, existingTableSchema, records);
		recordDao.batchUpsert(instanceId, recordTypeName, records, updatedSchema);
		RecordResponse response = new RecordResponse(recordId, recordType, singleRecord.getAttributes(),
				new RecordMetadata("TODO: SUPERFRESH"));
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	private Map<String, DataTypeMapping> addOrUpdateColumnIfNeeded(UUID instanceId, String recordType,
			Map<String, DataTypeMapping> schema, Map<String, DataTypeMapping> existingTableSchema,
			List<Record> records) {
		MapDifference<String, DataTypeMapping> difference = Maps.difference(existingTableSchema, schema);
		Map<String, DataTypeMapping> colsToAdd = difference.entriesOnlyOnRight();
		colsToAdd.keySet().stream().filter(s -> s.startsWith(RESERVED_NAME_PREFIX)).findAny().ifPresent(s -> {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Attribute names can't begin with " + RESERVED_NAME_PREFIX);
		});
		Set<Relation> relations = RelationUtils.findRelations(records);
		Map<String, List<Relation>> newRefCols = relations.stream()
				.collect(Collectors.groupingBy(Relation::relationColName));
		// TODO: better communicate to the user that they're trying to assign multiple
		// record types to a
		// single column
		Preconditions.checkArgument(newRefCols.values().stream().filter(l -> l.size() > 1).findAny().isEmpty());
		for (String col : colsToAdd.keySet()) {
			String referencedRecordType = null;
			if (newRefCols.containsKey(col)) {
				referencedRecordType = newRefCols.get(col).get(0).relationRecordType().getName();
			}
			recordDao.addColumn(instanceId, recordType, col, colsToAdd.get(col), referencedRecordType);
			schema.put(col, colsToAdd.get(col));
		}
		if (!recordDao.getRelationCols(instanceId, recordType).stream().map(Relation::relationColName)
				.collect(Collectors.toSet()).containsAll(newRefCols.keySet())) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"It looks like you're attempting to assign a relation "
							+ "to an existing column that was not configured for relations");
		}
		Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap = difference.entriesDiffering();
		for (String column : differenceMap.keySet()) {
			MapDifference.ValueDifference<DataTypeMapping> valueDifference = differenceMap.get(column);
			DataTypeMapping updatedColType = inferer.selectBestType(valueDifference.leftValue(),
					valueDifference.rightValue());
			recordDao.changeColumn(instanceId, recordType, column, updatedColType);
			schema.put(column, updatedColType);
		}
		return schema;
	}

	@GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> getSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId) {
		validateVersion(version);
		if (!recordDao.instanceSchemaExists(instanceId)
				|| !recordDao.recordTypeExists(instanceId, recordType.getName())) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Instance or table don't exist");
		}
		Record result = recordDao
				.getSingleRecord(instanceId, recordType, recordId,
						recordDao.getRelationCols(instanceId, recordType.getName()))
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Record not found"));
		RecordResponse response = new RecordResponse(recordId, recordType, result.getAttributes(),
				new RecordMetadata("TODO: RECORDMETADATA"));
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> upsertSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		String recordTypeName = recordType.getName();
		Map<String, Object> attributesInRequest = recordRequest.recordAttributes().getAttributes();
		Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest);
		if (!recordDao.instanceSchemaExists(instanceId)) {
			recordDao.createSchema(instanceId);
		}
		if (!recordDao.recordTypeExists(instanceId, recordTypeName)) {
			RecordResponse response = new RecordResponse(recordId, recordType, recordRequest.recordAttributes(),
					new RecordMetadata("TODO"));
			Record newRecord = new Record(recordId, recordType, recordRequest);
			createRecordTypeAndInsertRecords(instanceId, newRecord, recordTypeName, requestSchema);
			return new ResponseEntity<>(response, HttpStatus.CREATED);
		} else {
			Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchema(instanceId,
					recordTypeName);
			// null out any attributes that already exist but aren't in the request
			existingTableSchema.keySet().forEach(attr -> attributesInRequest.putIfAbsent(attr, null));
			Record record = new Record(recordId, recordType, recordRequest.recordAttributes());
			List<Record> records = Collections.singletonList(record);
			addOrUpdateColumnIfNeeded(instanceId, recordType.getName(), requestSchema, existingTableSchema, records);
			Map<String, DataTypeMapping> combinedSchema = new HashMap<>(existingTableSchema);
			combinedSchema.putAll(requestSchema);
			recordDao.batchUpsert(instanceId, recordTypeName, records, combinedSchema);
			RecordResponse response = new RecordResponse(recordId, recordType,
					new RecordAttributes(attributesInRequest), new RecordMetadata("TODO"));
			return new ResponseEntity<>(response, HttpStatus.OK);
		}
	}

	@PostMapping("/{instanceId}/{version}/")
	public ResponseEntity<String> createInstance(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version) {
		validateVersion(version);
		if (recordDao.instanceSchemaExists(instanceId)) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "This schema already exists");
		}
		recordDao.createSchema(instanceId);
		return new ResponseEntity<>(HttpStatus.CREATED);
	}

	@DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity deleteSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId) {
		validateVersion(version);
		String recordTypeName = recordType.getName();
		recordDao
				.getSingleRecord(instanceId, recordType, recordId,
						recordDao.getRelationCols(instanceId, recordTypeName))
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Record not found"));
		recordDao.deleteSingleRecord(instanceId, recordType.getName(), recordId.getRecordIdentifier());
		return new ResponseEntity<>(HttpStatus.OK);
	}

	private static void validateVersion(String version) {
		if (null == version || !version.equals("v0.2")) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid API version specified");
		}
	}

	private void createRecordTypeAndInsertRecords(UUID instanceId, Record newRecord, String recordTypeName,
			Map<String, DataTypeMapping> requestSchema) {
		if (recordTypeName.startsWith(RESERVED_NAME_PREFIX)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Record types can't start with " + RESERVED_NAME_PREFIX);
		}
		List<Record> records = Collections.singletonList(newRecord);
		recordDao.createRecordType(instanceId, requestSchema, recordTypeName, RelationUtils.findRelations(records));
		recordDao.batchUpsert(instanceId, recordTypeName, records, requestSchema);
	}
}
