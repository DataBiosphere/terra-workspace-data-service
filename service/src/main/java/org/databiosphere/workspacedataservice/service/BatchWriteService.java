package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;

import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.RecordDao;
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
  public Map<RecordType, Integer> consumeWriteStream(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType,
      Optional<String> primaryKey) {
    //    int recordsAffected = 0;
    Map<RecordType, Integer> result = new HashMap<>();
    try {
      Map<String, DataTypeMapping> schema = null;
      boolean firstUpsertBatch = true;
      for (StreamingWriteHandler.WriteStreamInfo info =
              streamingWriteHandler.readRecords(batchSize);
          !info.getRecords().isEmpty();
          info = streamingWriteHandler.readRecords(batchSize)) {
        List<Record> records = info.getRecords();
        /// TEST
        // TODO how to deal with firstUpsertBatch
        // TODO is a recordType == null check the best way to do this?
        if (recordType == null) {
          Map<RecordType, List<Record>> sortedRecords =
              records.stream().collect(Collectors.groupingBy(Record::getRecordType));
          for (Map.Entry<RecordType, List<Record>> recList : sortedRecords.entrySet()) {
            //            int recordsAffected =
            processRecords(
                true,
                info.getOperationType(),
                recList.getValue(),
                schema,
                instanceId,
                recList.getKey(),
                primaryKey);
            result.compute(
                recList.getKey(),
                (key, value) ->
                    (value == null)
                        ? recList.getValue().size()
                        : value + recList.getValue().size());
          }
        } else {
          result.putIfAbsent(recordType, 0);
          firstUpsertBatch =
              processRecords(
                  true,
                  info.getOperationType(),
                  records,
                  schema,
                  instanceId,
                  recordType,
                  primaryKey);
          ///
          /// ORIGINAL CODE
          //          if (firstUpsertBatch && info.getOperationType() == OperationType.UPSERT) {
          //            schema = inferer.inferTypes(records);
          //            createOrModifyRecordType(
          //                instanceId, recordType, schema, records, primaryKey.orElse(RECORD_ID));
          //            firstUpsertBatch = false;
          //          }
          //          writeBatch(instanceId, recordType, schema, info.getOperationType(), records,
          // primaryKey);
          //          recordsAffected += records.size();
          result.compute(
              recordType,
              (key, value) -> (value == null) ? records.size() : value + records.size());

          ////
        }
      }
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
    //    return recordsAffected;
    return result;
  }

  private boolean processRecords(
      boolean firstUpsertBatch,
      OperationType opType,
      List<Record> records,
      Map<String, DataTypeMapping> schema,
      UUID instanceId,
      RecordType recordType,
      Optional<String> primaryKey) {
    if (firstUpsertBatch && opType == OperationType.UPSERT) {
      // TODO should we infer types every time for multi-record type for changes?  Probably I guess?
      schema = inferer.inferTypes(records);
      createOrModifyRecordType(
          instanceId, recordType, schema, records, primaryKey.orElse(RECORD_ID));
      firstUpsertBatch = false;
    }
    writeBatch(instanceId, recordType, schema, opType, records, primaryKey);
    return firstUpsertBatch;
  }

  // try-with-resources wrapper for JsonStreamWriteHandler; calls consumeWriteStream.
  @WriteTransaction
  public int batchWriteJsonStream(
      InputStream is, UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
    try (StreamingWriteHandler streamingWriteHandler =
        new JsonStreamWriteHandler(is, objectMapper)) {
      return consumeWriteStream(streamingWriteHandler, instanceId, recordType, primaryKey)
          .get(recordType);
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
          .get(recordType);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  @WriteTransaction
  public Map<RecordType, Integer> batchWritePfbStream(
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
