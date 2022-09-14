package org.databiosphere.workspacedataservice.controller;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.*;
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
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			throw new MissingObjectException("Record type");
		}
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
			throw new InvalidNameException("Attribute");
		});
		validateRelationsAndAddColumns(instanceId, recordType, schema, records, colsToAdd, existingTableSchema);
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
		for (String col : colsToAdd.keySet()) {
			RecordType referencedRecordType = null;
			if (allRefCols.containsKey(col)) {
				referencedRecordType = allRefCols.get(col).get(0).relationRecordType();
			}
			recordDao.addColumn(instanceId, recordType, col, colsToAdd.get(col), referencedRecordType);
			requestSchema.put(col, colsToAdd.get(col));
		}
	}

	@GetMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> getSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId) {
		validateVersion(version);
		if (!recordDao.instanceSchemaExists(instanceId)) {
			throw new MissingObjectException("Instance");
		}
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			throw new MissingObjectException("Record type");
		}
		Record result = recordDao
				.getSingleRecord(instanceId, recordType, recordId, recordDao.getRelationCols(instanceId, recordType))
				.orElseThrow(() -> new MissingObjectException("Record"));
		RecordResponse response = new RecordResponse(recordId, recordType, result.getAttributes(),
				new RecordMetadata("TODO: RECORDMETADATA"));
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PutMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity<RecordResponse> upsertSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId, @RequestBody RecordRequest recordRequest) {
		validateVersion(version);
		// String recordTypeName = recordType;
		Map<String, Object> attributesInRequest = recordRequest.recordAttributes().getAttributes();
		Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest);
		if (!recordDao.instanceSchemaExists(instanceId)) {
			recordDao.createSchema(instanceId);
		}
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			RecordResponse response = new RecordResponse(recordId, recordType, recordRequest.recordAttributes(),
					new RecordMetadata("TODO"));
			Record newRecord = new Record(recordId, recordType, recordRequest);
			createRecordTypeAndInsertRecords(instanceId, newRecord, recordType, requestSchema);
			return new ResponseEntity<>(response, HttpStatus.CREATED);
		} else {
			Map<String, DataTypeMapping> existingTableSchema = recordDao.getExistingTableSchema(instanceId, recordType);
			// null out any attributes that already exist but aren't in the request
			existingTableSchema.keySet().forEach(attr -> attributesInRequest.putIfAbsent(attr, null));
			Record record = new Record(recordId, recordType, recordRequest.recordAttributes());
			List<Record> records = Collections.singletonList(record);
			addOrUpdateColumnIfNeeded(instanceId, recordType, requestSchema, existingTableSchema, records);
			Map<String, DataTypeMapping> combinedSchema = new HashMap<>(existingTableSchema);
			combinedSchema.putAll(requestSchema);
			recordDao.batchUpsert(instanceId, recordType, records, combinedSchema);
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
			throw new ResponseStatusException(HttpStatus.CONFLICT, "This instance already exists");
		}
		recordDao.createSchema(instanceId);
		return new ResponseEntity<>(HttpStatus.CREATED);
	}

	@DeleteMapping("/{instanceId}/records/{version}/{recordType}/{recordId}")
	public ResponseEntity deleteSingleRecord(@PathVariable("instanceId") UUID instanceId,
			@PathVariable("version") String version, @PathVariable("recordType") RecordType recordType,
			@PathVariable("recordId") RecordId recordId) {
		validateVersion(version);
		boolean recordFound = recordDao.deleteSingleRecord(instanceId, recordType, recordId.getRecordIdentifier());
		return recordFound ? new ResponseEntity<>(HttpStatus.NO_CONTENT) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
	}

	private static void validateVersion(String version) {
		if (null == version || !version.equals("v0.2")) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid API version specified");
		}
	}

	private void createRecordTypeAndInsertRecords(UUID instanceId, Record newRecord, RecordType recordType,
			Map<String, DataTypeMapping> requestSchema) {
		recordType.validate();
		List<Record> records = Collections.singletonList(newRecord);
		recordDao.createRecordType(instanceId, requestSchema, recordType, RelationUtils.findRelations(records));
		recordDao.batchUpsert(instanceId, recordType, records, requestSchema);
	}
}
