package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode.BASE_ATTRIBUTES;
import static org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode.RELATIONS;

import bio.terra.common.db.WriteTransaction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.databiosphere.workspacedataservice.recordsink.RecordSink;
import org.databiosphere.workspacedataservice.recordsource.RecordSource;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.WriteStreamInfo;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BadStreamingWriteRequestException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

@Service
public class BatchWriteService {
  private final DataTypeInferer inferer;
  private final int batchSize;

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriteService.class);

  public BatchWriteService(
      @Value("${twds.write.batch.size:5000}") int batchSize, DataTypeInferer inf) {
    this.batchSize = batchSize;
    this.inferer = inf;
  }

  /**
   * Responsible for looping over and upserting batches of Records found in the provided {@link
   * RecordSource}.
   *
   * @param recordSource the source of the records to be upserted
   * @param recordType record type of records contained in the write handler
   * @param primaryKey primaryKey column for the record type
   * @return a {@link BatchWriteResult} with metadata about the written records
   */
  @WriteTransaction
  public BatchWriteResult batchWrite(
      RecordSource recordSource,
      RecordSink recordSink,
      @Nullable RecordType recordType,
      String primaryKey) {
    try (recordSource) {
      return consumeWriteStream(recordSource, recordSink, recordType, primaryKey);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  private BatchWriteResult consumeWriteStream(
      RecordSource recordSource,
      RecordSink recordSink,
      @Nullable RecordType recordType,
      String primaryKey)
      throws IOException {
    BatchWriteResult result = BatchWriteResult.empty();

    // tracker to stash the schemas for the record types seen while processing this stream
    Map<RecordType, Map<String, DataTypeMapping>> typeSchemas = new HashMap<>();

    // loop through, in batches, the records provided by the RecordSource. This loops
    // until the RecordSource returns an empty batch.
    for (WriteStreamInfo info = recordSource.readRecords(batchSize);
        !info.records().isEmpty();
        info = recordSource.readRecords(batchSize)) {
      // Group the incoming records by their record types. TDR and PFB inputs expect to have
      // multiple types within the same input stream. TSV and JSON are expected to have a single
      // record type, so this will result in a grouping of 1.
      Multimap<RecordType, Record> groupedRecords =
          Multimaps.index(info.records(), Record::getRecordType);

      // TSV and JSON inputs are validated against the recordType argument. PFB inputs pass
      // a null recordType argument so there is nothing to validate.
      assertRecordTypesMatch(recordType, groupedRecords.keySet());

      // loop over all record types in this batch. For each record type, iff this is the first
      // time we've seen this type, calculate a schema from its records and update the record type
      // as necessary. Then, write the records into the table.
      OperationType opType = info.operationType();
      for (RecordType recType : groupedRecords.keySet()) {
        // despite its name, copyOf avoids copying if possible
        List<Record> records = ImmutableList.copyOf(groupedRecords.get(recType));

        // infer and update this record type's schema, then save that schema back to the
        // `typeSchemas` map
        if (opType == OperationType.UPSERT) {
          Map<String, DataTypeMapping> inferredSchema = inferer.inferTypes(records);
          Map<String, DataTypeMapping> finalSchema =
              recordSink.createOrModifyRecordType(recType, inferredSchema, records, primaryKey);
          typeSchemas.put(recType, finalSchema);
        }

        Map<String, DataTypeMapping> schema = typeSchemas.get(recType);
        // when updating relations only, do not update if there are no relations
        if (recordSource.importMode() == BASE_ATTRIBUTES || !schema.isEmpty()) {
          // For relations only, remove records that have no relations
          var recordsToWrite =
              recordSource.importMode() == RELATIONS ? excludeEmptyRecords(records) : records;

          switch (opType) {
            case UPSERT -> {
              LOGGER.info(
                  "Upserting {} records as {} for record type {}",
                  recordsToWrite.size(),
                  recordSource.importMode().name(),
                  recType.getName());
              recordSink.upsertBatch(recType, schema, recordsToWrite, primaryKey);
            }
            case DELETE -> recordSink.deleteBatch(recType, recordsToWrite);
            default -> throw new UnsupportedOperationException(
                "OperationType " + opType + " is not supported");
          }
          // update the result counts
          result.increaseCount(recType, recordsToWrite.size());
        } else {
          LOGGER.info("Nothing to import for this batch for record type {}", recType.getName());
        }
      }
    }
    return result;
  }

  private static void assertRecordTypesMatch(
      @Nullable RecordType recordType, Set<RecordType> recordTypes) {
    if (recordType != null && !Set.of(recordType).equals(recordTypes)) {
      throw new BadStreamingWriteRequestException(
          "Record Type was specified as argument to BatchWriteService, "
              + "but actual records contained different record types. Cannot continue.");
    }
  }

  private static List<Record> excludeEmptyRecords(List<Record> recordsForType) {
    return recordsForType.stream().filter(rec -> !rec.attributeSet().isEmpty()).toList();
  }
}
