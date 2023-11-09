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
  private BatchWriteResult consumeWriteStream(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType,
      Optional<String> primaryKey) {
    BatchWriteResult result = BatchWriteResult.empty();
    try {
      // tracker to stash the schemas for the record types seen while processing this stream
      // TODO AJ-1227: unit tests for typesSeen feature
      Map<RecordType, Map<String, DataTypeMapping>> typesSeen = new HashMap<>();

      // loop through, in batches, the records provided by the StreamingWriteHandler. This loops
      // until the StreamingWriteHandler returns an empty batch.
      for (StreamingWriteHandler.WriteStreamInfo info =
              streamingWriteHandler.readRecords(batchSize);
          !info.getRecords().isEmpty();
          info = streamingWriteHandler.readRecords(batchSize)) {
        // get the records for this batch
        List<Record> records = info.getRecords();

        // If no recordType argument is given, assume there may be multiple types and sort by type.
        // If recordType is specified, assume all records are of that type.
        // TODO AJ-1227: any reason to not just sort/group in all cases? I am hesitant to change
        //    the legacy behavior of the recordType argument, but this if/then seems redundant
        Map<RecordType, List<Record>> sortedRecords;
        if (recordType == null) {
          sortedRecords = records.stream().collect(Collectors.groupingBy(Record::getRecordType));
        } else {
          sortedRecords = new HashMap<>();
          sortedRecords.put(recordType, records);
        }

        // loop over all record types in this batch. For each record type, iff this is the first
        // time we've seen this type, calculate a schema from its records and update the record type
        // as necessary. Then, write the records into the table.
        // TODO AJ-1452: for PFB imports, get schema from Avro, not from attribute values inference
        for (Map.Entry<RecordType, List<Record>> recList : sortedRecords.entrySet()) {
          RecordType recType = recList.getKey();
          List<Record> rList = recList.getValue();
          // have we already processed at least one batch of this record type?
          boolean isTypeAlreadySeen = typesSeen.containsKey(recType);
          // if this is the first time we've seen this record type, infer and update this record
          // type's schema, then save that schema back to the `typesSeen` map
          if (!isTypeAlreadySeen && info.getOperationType() == OperationType.UPSERT) {
            Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(rList);
            Map<String, DataTypeMapping> finalSchema =
                createOrModifyRecordType(
                    instanceId, recType, inferredSchema, rList, primaryKey.orElse(RECORD_ID));
            typesSeen.put(recType, finalSchema);
          }
          // write these records to the db, using the schema from the `typesSeen` map
          writeBatch(
              instanceId,
              recType,
              typesSeen.get(recType),
              info.getOperationType(),
              rList,
              primaryKey);
          // update the result counts
          result.increaseCount(recType, rList.size());
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
