package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;

import bio.terra.common.db.WriteTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimaps;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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

  private BatchWriteResult consumeWriteStream(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType,
      Optional<String> primaryKey) {
    return consumeWriteStreamWithRelations(
        streamingWriteHandler,
        instanceId,
        recordType,
        primaryKey,
        PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES);
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
  private BatchWriteResult consumeWriteStreamWithRelations(
      StreamingWriteHandler streamingWriteHandler,
      UUID instanceId,
      RecordType recordType,
      Optional<String> primaryKey,
      PfbStreamWriteHandler.PfbImportMode pfbImportMode) {
    BatchWriteResult result = BatchWriteResult.empty();
    try {
      // Verify relationsOnly is only for pfbstreamingwriteHandler
      if (pfbImportMode == PfbStreamWriteHandler.PfbImportMode.RELATIONS
          && !(streamingWriteHandler instanceof PfbStreamWriteHandler)) {
        throw new BadStreamingWriteRequestException(
            "BatchWriteService attempted to re-read PFB "
                + "on a non-PFB import. Cannot continue.");
      }

      // tracker to stash the schemas for the record types seen while processing this stream
      Map<RecordType, Map<String, DataTypeMapping>> typesSeen = new HashMap<>();

      // loop through, in batches, the records provided by the StreamingWriteHandler. This loops
      // until the StreamingWriteHandler returns an empty batch.
      for (StreamingWriteHandler.WriteStreamInfo info =
              streamingWriteHandler.readRecords(batchSize);
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
                createOrModifyRecordType(
                    instanceId, recType, inferredSchema, rList, primaryKey.orElse(RECORD_ID));
            typesSeen.put(recType, finalSchema);
          }
          // when updating relations only, do not update if there are no relations
          if (pfbImportMode == PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES
              || !typesSeen.get(recType).isEmpty()) {
            if (pfbImportMode == PfbStreamWriteHandler.PfbImportMode.RELATIONS) {
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

  /**
   * Persist records from a PFB into WDS's database.
   *
   * <p>As of this writing, the only caller of this method is PfbQuartzJob, and the only use case is
   * import from the UCSC AnVIL Data Browser. In this single use case, the `primaryKey` argument
   * will always be `Optional.of("id")`. However, as we add use cases and import PFBs from other
   * providers, this may change, and we will encounter different `primaryKey` argument values.
   *
   * @param is the PFB/Avro stream
   * @param instanceId WDS instance into which to import
   * @param primaryKey where to find the primary key for records in the PFB/Avro stream
   * @return counts of updated records, grouped by record type
   */
  /*
   *
   */
  @WriteTransaction
  public BatchWriteResult batchWritePfbStream(
      DataFileStream<GenericRecord> is,
      UUID instanceId,
      Optional<String> primaryKey,
      PfbStreamWriteHandler.PfbImportMode pfbImportMode) {
    try (PfbStreamWriteHandler streamingWriteHandler =
        new PfbStreamWriteHandler(is, pfbImportMode)) {
      return consumeWriteStreamWithRelations(
          streamingWriteHandler, instanceId, null, primaryKey, pfbImportMode);
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
