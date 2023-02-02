package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
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
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.tsv.TsvConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

	@WriteTransaction
	public int uploadTsvStream(InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) throws IOException {
		MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(is);

		// check for no rows in TSV
		if (!tsvIterator.hasNext()) {
			throw new InvalidTsvException("We could not parse any data rows in your tsv file.");
		}

		// extract column names from the schema
		List<String> colNames;
		FormatSchema formatSchema = tsvIterator.getParser().getSchema();
		if (formatSchema instanceof CsvSchema actualSchema) {
			colNames = actualSchema.getColumnNames();
		} else {
			throw new InvalidTsvException("Could not determine primary key column; unexpected schema type:" + formatSchema.getSchemaType());
		}

		// if a primary key is specified, check if it is present in the TSV
		if (primaryKey.isPresent() && !colNames.contains(primaryKey.get())) {
			throw new InvalidTsvException(
					"Uploaded TSV is either missing the " + primaryKey
							+ " column or has a null or empty string value in that column");
		}

		// if primary key is not specified, use the leftmost column
		String resolvedPK = primaryKey.orElseGet( () -> colNames.get(0) );

		// convert the tsvIterator, which is a MappingIterator<RecordAttributes>, to a Stream<Record>
		TsvConverter tsvConverter = new TsvConverter();
		Stream<RecordAttributes> tsvStream = StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(tsvIterator, Spliterator.ORDERED), false);
		Stream<Record> recordStream = tsvConverter.rowsToRecords(tsvStream, recordType, resolvedPK);

		// hand off the Stream<Record> to be batch-written
		return batchWriteTsvStream(recordStream, instanceId, recordType, Optional.of(resolvedPK));
	}

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
	@WriteTransaction
	public int batchWriteJsonStream(InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		try (StreamingWriteHandler streamingWriteHandler = new JsonStreamWriteHandler(is, objectMapper)) {
			return consumeWriteStream(streamingWriteHandler, instanceId, recordType, primaryKey);
		} catch (IOException e) {
			throw new BadStreamingWriteRequestException(e);
		}
	}

	@WriteTransaction
	public int batchWriteTsvStream(Stream<Record> upsertRecords, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
		try (StreamingWriteHandler streamingWriteHandler = new TsvStreamWriteHandler(upsertRecords)) {
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
