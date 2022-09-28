package org.databiosphere.workspacedataservice.service;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

@Service
public class BatchWriteService {

	private final RecordDao recordDao;

	private final DataTypeInferer inferer = new DataTypeInferer();

	private final int batchSize;

	public BatchWriteService(RecordDao recordDao, @Value("${twds.write.batch.size}") int batchSize) {
		this.recordDao = recordDao;
		this.batchSize = batchSize;
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

	/**
	 * All or nothing, write all the operations successfully in the InputStream or write none.
	 * @param is
	 * @param instanceId
	 * @param recordType
	 * @return number of records updated
	 */
	@Transactional
	public int consumeWriteStream(InputStream is, UUID instanceId, RecordType recordType) {
		int recordsAffected = 0;
		try (StreamingWriteHandler streamingWriteHandler = new StreamingWriteHandler(is)) {
			Map<String, DataTypeMapping> schema = null;
			boolean firstBatch = true;
			for (StreamingWriteHandler.WriteStreamInfo info = streamingWriteHandler.readRecords(batchSize); !info
					.getRecords().isEmpty(); info = streamingWriteHandler.readRecords(batchSize)) {
				List<Record> records = info.getRecords();
				if (firstBatch && info.getOperationType() == OperationType.UPSERT) {
					schema = inferer.inferTypes(records);
					createOrModifyRecordType(instanceId, recordType, schema, records);
					firstBatch = false;
				}
				writeBatch(instanceId, recordType, schema, info, records);
				recordsAffected += records.size();
			}
		} catch (IOException e) {
			throw new BadStreamingWriteRequestException(e);
		}
		return recordsAffected;
	}

	private void createOrModifyRecordType(UUID instanceId, RecordType recordType, Map<String, DataTypeMapping> schema, List<Record> records) {
		if (!recordDao.instanceSchemaExists(instanceId)) {
			recordDao.createSchema(instanceId);
		}
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			recordDao.createRecordType(instanceId, schema, recordType,
					RelationUtils.findRelations(records));
		} else {
			addOrUpdateColumnIfNeeded(instanceId, recordType, schema,
					recordDao.getExistingTableSchema(instanceId, recordType), records);
		}
	}

	private void writeBatch(UUID instanceId, RecordType recordType, Map<String, DataTypeMapping> schema,
			StreamingWriteHandler.WriteStreamInfo info, List<Record> records) throws BatchWriteException {
		if (info.getOperationType() == OperationType.UPSERT) {
			recordDao.batchUpsertWithErrorCapture(instanceId, recordType, records, schema);
		} else if (info.getOperationType() == OperationType.DELETE) {
			recordDao.batchDelete(instanceId, recordType, records);
		}
	}
}
