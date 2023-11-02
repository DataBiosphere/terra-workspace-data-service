package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;

import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class BatchWriteService {

  private final RecordDao recordDao;

  private final DataTypeInferer inferer;

  private final int batchSize;

  private final ObjectMapper objectMapper;

  private final ObjectReader tsvReader;

  private final RecordService recordService;

  public BatchWriteService(
      RecordDao recordDao,
      @Value("${twds.write.batch.size:5000}") int batchSize,
      DataTypeInferer inf,
      ObjectMapper objectMapper,
      ObjectReader tsvReader,
      RecordService recordService) {
    this.recordDao = recordDao;
    this.batchSize = batchSize;
    this.inferer = inf;
    this.objectMapper = objectMapper;
    this.tsvReader = tsvReader;
    this.recordService = recordService;
  }

  // TODO should the map response be its own object?  BatchResponse?
  /**
   * Responsible for accepting either a JsonStreamWriteHandler or a TsvStreamWriteHandler, looping
   * over the batches of Records found in the handler, and upserting those records.
   *
   * @param streamingWriteHandler the JsonStreamWriteHandler or a TsvStreamWriteHandler
   * @param instanceId instance to which records are upserted
   * @param recordType record type of records contained in the write handler
   * @param primaryKey PK for the record type
   * @return the number of records written
   */
  public BatchWriteResult consumeWriteStream(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType,
      Optional<String> primaryKey) {
    BatchWriteResult result = BatchWriteResult.empty();
    try {
      Map<String, DataTypeMapping> schema = null;
      boolean firstUpsertBatch = true;
      for (StreamingWriteHandler.WriteStreamInfo info =
              streamingWriteHandler.readRecords(batchSize);
          !info.getRecords().isEmpty();
          info = streamingWriteHandler.readRecords(batchSize)) {
        List<Record> records = info.getRecords();
        // TODO is a recordType == null check the best way to do this?
        // If no recordType is given, assume there may be multiple types and sort by type
        if (recordType == null) {
          Map<RecordType, List<Record>> sortedRecords =
              records.stream().collect(Collectors.groupingBy(Record::getRecordType));
          for (Map.Entry<RecordType, List<Record>> recList : sortedRecords.entrySet()) {
            RecordType recType = recList.getKey();
            List<Record> rList = recList.getValue();
            Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(rList);
            createOrModifyRecordType(
                instanceId, recType, inferredSchema, rList, primaryKey.orElse(RECORD_ID));
            writeBatch(
                instanceId, recType, inferredSchema, info.getOperationType(), rList, primaryKey);
            result.increaseCount(recType, rList.size());
          }
        } else {
          // A recordType has been passed in so all records are of this single type
          result.initialize(recordType);
          if (firstUpsertBatch && info.getOperationType() == OperationType.UPSERT) {
            schema = inferer.inferTypes(records);
            createOrModifyRecordType(
                instanceId, recordType, schema, records, primaryKey.orElse(RECORD_ID));
            firstUpsertBatch = false;
          }
          writeBatch(instanceId, recordType, schema, info.getOperationType(), records, primaryKey);
          result.increaseCount(recordType, records.size());
        }
      }
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
    return result;
  }

  // try-with-resources wrapper for JsonStreamWriteHandler; calls consumeWriteStream.
  @WriteTransaction
  public int batchWriteJsonStream(
      InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
    try (StreamingWriteHandler streamingWriteHandler =
        new JsonStreamWriteHandler(is, objectMapper)) {
      return consumeWriteStream(streamingWriteHandler, instanceId, recordType, primaryKey)
          .getUpdatedCount(recordType);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  // try-with-resources wrapper for TsvStreamWriteHandler; calls consumeWriteStream.
  @WriteTransaction
  public int batchWriteTsvStream(
      InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
    try (TsvStreamWriteHandler streamingWriteHandler =
        new TsvStreamWriteHandler(is, tsvReader, recordType, primaryKey)) {
      return consumeWriteStream(
              streamingWriteHandler,
              instanceId,
              recordType,
              Optional.of(streamingWriteHandler.getResolvedPrimaryKey()))
          .getUpdatedCount(recordType);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  @WriteTransaction
  public BatchWriteResult batchWritePfbStream(
      DataFileStream<GenericRecord> is, UUID instanceId, Optional<String> primaryKey) {
    try (PfbStreamWriteHandler streamingWriteHandler = new PfbStreamWriteHandler(is)) {
      return consumeWriteStream(streamingWriteHandler, instanceId, null, primaryKey);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  private Map<String, DataTypeMapping> createOrModifyRecordType(
      UUID instanceId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String recordTypePrimaryKey) {
    if (!recordDao.recordTypeExists(instanceId, recordType)) {
      recordDao.createRecordType(
          instanceId,
          schema,
          recordType,
          inferer.findRelations(records, schema),
          recordTypePrimaryKey);
    } else {
      return recordService.addOrUpdateColumnIfNeeded(
          instanceId,
          recordType,
          schema,
          recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType),
          records);
    }
    return schema;
  }

  private void writeBatch(
      UUID instanceId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      OperationType opType,
      List<Record> records,
      Optional<String> primaryKey)
      throws BatchWriteException {
    if (opType == OperationType.UPSERT) {
      recordService.batchUpsertWithErrorCapture(
          instanceId, recordType, records, schema, primaryKey.orElse(RECORD_ID));
    } else if (opType == OperationType.DELETE) {
      recordDao.batchDelete(instanceId, recordType, records);
    }
  }
}
