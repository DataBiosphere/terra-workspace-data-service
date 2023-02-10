package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

@Service
public class BatchWriteService {

	private final RecordDao recordDao;

	private final DataTypeInferer inferer;

	private final int batchSize;

	private final ObjectMapper objectMapper;

	private final ObjectReader tsvReader;

	private final RecordService recordService;

	private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriteService.class);

	public BatchWriteService(RecordDao recordDao, @Value("${twds.write.batch.size:5000}") int batchSize, DataTypeInferer inf, ObjectMapper objectMapper, ObjectReader tsvReader, RecordService recordService) {
		this.recordDao = recordDao;
		this.batchSize = batchSize;
		this.inferer = inf;
		this.objectMapper = objectMapper;
		this.tsvReader = tsvReader;
		this.recordService = recordService;
	}

	public Map<String, DataTypeMapping> addOrUpdateColumnIfNeeded(UUID instanceId, RecordType recordType,
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
			//Don't allow updating relation columns
			if (valueDifference.leftValue() == DataTypeMapping.ARRAY_OF_RELATION ||
					valueDifference.leftValue() == DataTypeMapping.RELATION ){
				throw new InvalidRelationException("Unable to update a relation or array of relation attribute type");
			}
			DataTypeMapping updatedColType = inferer.selectBestType(valueDifference.leftValue(),
					valueDifference.rightValue());
			recordDao.changeColumn(instanceId, recordType, column, updatedColType);
			schema.put(column, updatedColType);
		}
		return schema;
	}

	private void validateRelations(Set<Relation> existingRelations, Set<Relation> newRelations, Map<String, DataTypeMapping> existingSchema){
		Set<String> existingRelationCols = existingRelations.stream().map(Relation::relationColName)
				.collect(Collectors.toSet());
		// look for case where requested relation column already exists as a
		// non-relational column
		for (Relation relation : newRelations) {
			String col = relation.relationColName();
			if (!existingRelationCols.contains(col) && existingSchema.containsKey(col)) {
				throw new InvalidRelationException("It looks like you're attempting to assign a relation "
						+ "to an existing attribute that was not configured for relations");
			}
		}
	}

	private void validateRelationsAndAddColumns(UUID instanceId, RecordType recordType,
			Map<String, DataTypeMapping> requestSchema, List<Record> records, Map<String, DataTypeMapping> colsToAdd,
			Map<String, DataTypeMapping> existingSchema) {
		RelationCollection relations = inferer.findRelations(records, requestSchema);
		RelationCollection existingRelations = new RelationCollection(Set.copyOf(recordDao.getRelationCols(instanceId, recordType)), Set.copyOf(recordDao.getRelationArrayCols(instanceId, recordType)));
//		// look for case where requested relation column already exists as a
//		// non-relational column
		validateRelations(existingRelations.relations(), relations.relations(), existingSchema);
		// same for relation-array columns
		validateRelations(existingRelations.relationArrays(), relations.relationArrays(), existingSchema);
		relations.relations().addAll(existingRelations.relations());
		relations.relationArrays().addAll(existingRelations.relationArrays());
		Map<String, List<Relation>> allRefCols = relations.relations().stream()
				.collect(Collectors.groupingBy(Relation::relationColName));
		if (allRefCols.values().stream().anyMatch(l -> l.size() > 1)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Relation attribute can only be assigned to one record type");
		}
		Map<String, List<Relation>> allRefArrCols = relations.relationArrays().stream()
				.collect(Collectors.groupingBy(Relation::relationColName));
		if (allRefArrCols.values().stream().anyMatch(l -> l.size() > 1)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Relation array attribute can only be assigned to one record type");
		}
		for (Map.Entry<String, DataTypeMapping> entry : colsToAdd.entrySet()) {
			RecordType referencedRecordType = null;
			String col = entry.getKey();
			DataTypeMapping dataType = entry.getValue();
			if (allRefCols.containsKey(col)) {
				referencedRecordType = allRefCols.get(col).get(0).relationRecordType();
			}
			recordDao.addColumn(instanceId, recordType, col, dataType, referencedRecordType);
			if (allRefArrCols.containsKey(col)){
				referencedRecordType = allRefArrCols.get(col).get(0).relationRecordType();
				recordDao.createRelationJoinTable(instanceId, col, recordType, referencedRecordType);
			}
			requestSchema.put(col, dataType);
		}
	}

	/**
	 * Responsible for accepting either a JsonStreamWriteHandler or a TsvStreamWriteHandler, looping over the
	 * batches of Records found in the handler, and upserting those records.
	 *
	 * @param streamingWriteHandler the JsonStreamWriteHandler or a TsvStreamWriteHandler
	 * @param instanceId instance to which records are upserted
	 * @param recordType record type of records contained in the write handler
	 * @param primaryKey PK for the record type
	 * @return the number of records written
	 */
	@WriteTransaction
	public int consumeWriteStream(StreamingWriteHandler streamingWriteHandler, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		int recordsAffected = 0;
		try {
			Map<String, DataTypeMapping> schema = null;
			boolean firstUpsertBatch = true;
			for (StreamingWriteHandler.WriteStreamInfo info = streamingWriteHandler.readRecords(batchSize); !info
					.getRecords().isEmpty(); info = streamingWriteHandler.readRecords(batchSize)) {
				List<Record> records = info.getRecords();
				if (firstUpsertBatch && info.getOperationType() == OperationType.UPSERT) {
					schema = inferer.inferTypes(records);
					createOrModifyRecordType(instanceId, recordType, schema, records, primaryKey.orElse(RECORD_ID));
					firstUpsertBatch = false;
				}
				writeBatch(instanceId, recordType, schema, info, records, primaryKey);
				recordsAffected += records.size();
			}
		} catch (IOException e) {
			throw new BadStreamingWriteRequestException(e);
		}
		return recordsAffected;
	}



	// try-with-resources wrapper for JsonStreamWriteHandler; calls consumeWriteStream.
	@WriteTransaction
	public int batchWriteJsonStream(InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		try (StreamingWriteHandler streamingWriteHandler = new JsonStreamWriteHandler(is, objectMapper)) {
			return consumeWriteStream(streamingWriteHandler, instanceId, recordType, primaryKey);
		} catch (IOException e) {
			throw new BadStreamingWriteRequestException(e);
		}
	}

	// try-with-resources wrapper for TsvStreamWriteHandler; calls consumeWriteStream.
	@WriteTransaction
	public int batchWriteTsvStream(InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		try (StreamingWriteHandler streamingWriteHandler = new TsvStreamWriteHandler(is, tsvReader, recordType, primaryKey)) {
			return consumeWriteStream(streamingWriteHandler, instanceId, recordType, primaryKey);
		} catch (IOException e) {
			throw new BadStreamingWriteRequestException(e);
		}
	}

	private Map<String, DataTypeMapping> createOrModifyRecordType(UUID instanceId, RecordType recordType,
			Map<String, DataTypeMapping> schema, List<Record> records, String recordTypePrimaryKey) {
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			recordDao.createRecordType(instanceId, schema, recordType, inferer.findRelations(records, schema), recordTypePrimaryKey);
		} else {
			return addOrUpdateColumnIfNeeded(instanceId, recordType, schema,
					recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType), records);
		}
		return schema;
	}

	private void writeBatch(UUID instanceId, RecordType recordType, Map<String, DataTypeMapping> schema,
							StreamingWriteHandler.WriteStreamInfo info, List<Record> records, Optional<String> primaryKey) throws BatchWriteException {
		if (info.getOperationType() == OperationType.UPSERT) {
			recordService.batchUpsertWithErrorCapture(instanceId, recordType, records, schema, primaryKey.orElse(RECORD_ID));
		} else if (info.getOperationType() == OperationType.DELETE) {
			recordDao.batchDelete(instanceId, recordType, records);
		}
	}
}
