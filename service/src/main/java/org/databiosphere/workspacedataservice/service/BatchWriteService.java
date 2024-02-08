package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES;
import static org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler.ImportMode.RELATIONS;

import bio.terra.common.db.WriteTransaction;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimaps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.recordstream.StreamingWriteHandler;
import org.databiosphere.workspacedataservice.recordstream.StreamingWriteHandler.WriteStreamInfo;
import org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler;
import org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler.ImportMode;
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
  private final RecordService recordService;

  public BatchWriteService(
      RecordDao recordDao,
      @Value("${twds.write.batch.size:5000}") int batchSize,
      DataTypeInferer inf,
      RecordService recordService) {
    this.recordDao = recordDao;
    this.batchSize = batchSize;
    this.inferer = inf;
    this.recordService = recordService;
  }

  /**
   * Responsible for looping over and upserting batches of Records found in the provided {@link
   * StreamingWriteHandler}.
   *
   * @param streamingWriteHandler the source of the records to be upserted
   * @param instanceId instance to which records are upserted
   * @param recordType record type of records contained in the write handler
   * @param primaryKey primaryKey column for the record type
   * @return a {@link BatchWriteResult} with metadata about the written records
   */
  @WriteTransaction
  public BatchWriteResult batchWrite(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType,
      String primaryKey,
      ImportMode importMode) {
    try (streamingWriteHandler) {
      return consumeWriteStream(
          streamingWriteHandler, instanceId, recordType, primaryKey, importMode);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  private BatchWriteResult consumeWriteStream(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType, // nullable
      String primaryKey,
      ImportMode importMode) {
    BatchWriteResult result = BatchWriteResult.empty();
    try {
      if (importMode == RELATIONS
          && !(streamingWriteHandler instanceof TwoPassStreamingWriteHandler)) {
        throw new BadStreamingWriteRequestException(
            "BatchWriteService attempted to re-read input data, but this input is not configured "
                + "as re-readable. Cannot continue.");
      }

      // tracker to stash the schemas for the record types seen while processing this stream
      Map<RecordType, Map<String, DataTypeMapping>> typesSeen = new HashMap<>();

      // loop through, in batches, the records provided by the StreamingWriteHandler. This loops
      // until the StreamingWriteHandler returns an empty batch.
      for (WriteStreamInfo info = streamingWriteHandler.readRecords(batchSize);
          !info.getRecords().isEmpty();
          info = streamingWriteHandler.readRecords(batchSize)) {
        // get the records for this batch
        List<Record> records = info.getRecords();

        // Group the incoming records by their record types. PFB inputs expect to have multiple
        // types within the same input stream. TSV and JSON are expected to have a single record
        // type, so this will result in a grouping of 1.
        // TSV and JSON inputs are validated against the recordType argument. PFB inputs pass
        // a null recordType argument so there is nothing to validate.
        ImmutableMultimap<RecordType, Record> groupedRecords =
            Multimaps.index(records, Record::getRecordType);

        if (recordType != null && !Set.of(recordType).equals(groupedRecords.keySet())) {
          throw new BadStreamingWriteRequestException(
              "Record Type was specified as argument to BatchWriteService, "
                  + "but actual records contained different record types. Cannot continue.");
        }

        // loop over all record types in this batch. For each record type, iff this is the first
        // time we've seen this type, calculate a schema from its records and update the record type
        // as necessary. Then, write the records into the table.
        for (RecordType recType : groupedRecords.keySet()) {
          List<Record> rList = groupedRecords.get(recType).asList();
          // have we already processed at least one batch of this record type?
          boolean isTypeAlreadySeen = typesSeen.containsKey(recType);
          // if this is the first time we've seen this record type, infer and update this record
          // type's schema, then save that schema back to the `typesSeen` map
          if (!isTypeAlreadySeen && info.getOperationType() == OperationType.UPSERT) {
            Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(rList);
            Map<String, DataTypeMapping> finalSchema =
                createOrModifyRecordType(instanceId, recType, inferredSchema, rList, primaryKey);
            typesSeen.put(recType, finalSchema);
          }
          // when updating relations only, do not update if there are no relations
          if (importMode == BASE_ATTRIBUTES || !typesSeen.get(recType).isEmpty()) {
            if (importMode == RELATIONS) {
              // For relations only, remove records that have no relations
              rList = rList.stream().filter(rec -> !rec.attributeSet().isEmpty()).toList();
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
      }
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
    return result;
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
      String primaryKey)
      throws BatchWriteException {
    if (opType == OperationType.UPSERT) {
      recordService.batchUpsertWithErrorCapture(
          instanceId, recordType, records, schema, primaryKey);
    } else if (opType == OperationType.DELETE) {
      recordDao.batchDelete(instanceId, recordType, records);
    }
  }
}
