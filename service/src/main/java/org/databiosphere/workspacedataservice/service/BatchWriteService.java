package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
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

	private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriteService.class);

	public BatchWriteService(RecordDao recordDao, @Value("${twds.write.batch.size:5000}") int batchSize, DataTypeInferer inf, ObjectMapper objectMapper) {
		this.recordDao = recordDao;
		this.batchSize = batchSize;
		this.inferer = inf;
		this.objectMapper = objectMapper;
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

	@Transactional
	public int uploadTsvStream(InputStreamReader is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) throws IOException {
		CSVFormat csvFormat = TsvSupport.getUploadFormat();
		CSVParser rows = csvFormat.parse(is);
		String leftMostColumn = rows.getHeaderNames().get(0);
		List<Record> batch = new ArrayList<>();
		HashSet<String> recordIds = new HashSet<>();
		boolean firstUpsertBatch = true;
		Map<String, DataTypeMapping> schema = null;
		String uniqueIdentifierAsString = primaryKey.orElse(leftMostColumn);
		int recordsProcessed = 0;
		for (CSVRecord row : rows) {
			Map<String, Object> m = (Map) row.toMap();
			m.remove(uniqueIdentifierAsString);
			String recordId = row.get(uniqueIdentifierAsString);
			// validate that all record ids in this TSV are unique
			if (!recordIds.add(recordId)) {
				throw new InvalidTsvException("TSVs cannot contain duplicate primary key values");
			}
			changeEmptyStringsToNulls(m);
			try {
				batch.add(new Record(recordId, recordType, new RecordAttributes(m)));
			} catch (IllegalArgumentException ex) {
				LOGGER.error("IllegalArgument exception while reading tsv", ex);
				throw new InvalidTsvException(
						"Uploaded TSV is either missing the " + primaryKey
								+ " column or has a null or empty string value in that column");
			}
			recordsProcessed++;
			if (batch.size() >= batchSize) {
				if (firstUpsertBatch) {
					schema = createOrUpdateSchema(instanceId, recordType, batch, uniqueIdentifierAsString);
					firstUpsertBatch = false;
				}
				recordDao.batchUpsert(instanceId, recordType, batch, schema);
				batch.clear();
				// remove the following line to ensure that record ids are unique within the entire TSV,
				// instead of within a batch; this would have performance implications for very large TSVs.
				recordIds.clear();
			}
		}
		if (firstUpsertBatch) {
			if (batch.isEmpty()) {
				throw new InvalidTsvException("We could not parse any data rows in your tsv file.");
			}
			schema = createOrUpdateSchema(instanceId, recordType, batch, uniqueIdentifierAsString);
		}
		recordDao.batchUpsert(instanceId, recordType, batch, schema);
		return recordsProcessed;
	}

	/**
	 * Should only be called from the TSV upload path, convert empty strings in the
	 * TSV to nulls for storage in the database
	 * 
	 * @param m
	 */
	private void changeEmptyStringsToNulls(Map<String, Object> m) {
		for (Map.Entry<String, Object> entry : m.entrySet()) {
			if (entry.getValue().toString().isEmpty()) {
				m.put(entry.getKey(), null);
			}
		}
	}

	private Map<String, DataTypeMapping> createOrUpdateSchema(UUID instanceId, RecordType recordType,
			List<Record> batch, String recordTypePrimaryKey) {
		Map<String, DataTypeMapping> schema = inferer.inferTypes(batch, InBoundDataSource.TSV);
		return createOrModifyRecordType(instanceId, recordType, schema, batch, recordTypePrimaryKey);
	}

	/**
	 * All or nothing, write all the operations successfully in the InputStream or
	 * write none.
	 *
	 * @param is
	 * @param instanceId
	 * @param recordType
	 * @param primaryKey
	 * @return number of records updated
	 */
	@Transactional
	public int consumeWriteStream(InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		int recordsAffected = 0;
		try (StreamingWriteHandler streamingWriteHandler = new StreamingWriteHandler(is, objectMapper)) {
			Map<String, DataTypeMapping> schema = null;
			boolean firstUpsertBatch = true;
			for (StreamingWriteHandler.WriteStreamInfo info = streamingWriteHandler.readRecords(batchSize); !info
					.getRecords().isEmpty(); info = streamingWriteHandler.readRecords(batchSize)) {
				List<Record> records = info.getRecords();
				if (firstUpsertBatch && info.getOperationType() == OperationType.UPSERT) {
					schema = inferer.inferTypes(records, InBoundDataSource.JSON);
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

	private Map<String, DataTypeMapping> createOrModifyRecordType(UUID instanceId, RecordType recordType,
			Map<String, DataTypeMapping> schema, List<Record> records, String recordTypePrimaryKey) {
		if (!recordDao.recordTypeExists(instanceId, recordType)) {
			recordDao.createRecordType(instanceId, schema, recordType, RelationUtils.findRelations(records), recordTypePrimaryKey);
		} else {
			return addOrUpdateColumnIfNeeded(instanceId, recordType, schema,
					recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType), records);
		}
		return schema;
	}

	private void writeBatch(UUID instanceId, RecordType recordType, Map<String, DataTypeMapping> schema,
							StreamingWriteHandler.WriteStreamInfo info, List<Record> records, Optional<String> primaryKey) throws BatchWriteException {
		if (info.getOperationType() == OperationType.UPSERT) {
			recordDao.batchUpsertWithErrorCapture(instanceId, recordType, records, schema, primaryKey.orElse(RECORD_ID));
		} else if (info.getOperationType() == OperationType.DELETE) {
			recordDao.batchDelete(instanceId, recordType, records);
		}
	}
}
