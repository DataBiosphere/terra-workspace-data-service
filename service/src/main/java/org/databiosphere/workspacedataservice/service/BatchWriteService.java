package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.recordsource.TwoPassRecordSource.ImportMode.BASE_ATTRIBUTES;
import static org.databiosphere.workspacedataservice.recordsource.TwoPassRecordSource.ImportMode.RELATIONS;

import bio.terra.common.db.WriteTransaction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimaps;
import com.google.mu.util.stream.BiStream;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class BatchWriteService {
  private final DataTypeInferer inferer;
  private final int batchSize;

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
      RecordSource recordSource, RecordSink recordSink, RecordType recordType, String primaryKey) {
    try (recordSource) {
      return consumeWriteStream(recordSource, recordSink, recordType, primaryKey);
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
  }

  private BatchWriteResult consumeWriteStream(
      RecordSource recordSource,
      RecordSink recordSink,
      RecordType recordType, // nullable
      String primaryKey) {
    BatchWriteResult result = BatchWriteResult.empty();
    try {
      // tracker to stash the schemas for the record types seen while processing this stream
      Map<RecordType, Map<String, DataTypeMapping>> typesSeen = new HashMap<>();

      // loop through, in batches, the records provided by the RecordSource. This loops
      // until the RecordSource returns an empty batch.
      for (WriteStreamInfo info = recordSource.readRecords(batchSize);
          !info.records().isEmpty();
          info = recordSource.readRecords(batchSize)) {
        // get the records for this batch
        List<Record> records = info.records();

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
        OperationType opType = info.operationType();
        BiStream.from(groupedRecords.asMap())
            .mapValues(ImmutableList::copyOf) // despite its name, copyOf avoids copying if possible
            .forEach(
                (recType, recordsForType) -> {
                  // have we already processed at least one batch of this record type?
                  boolean isTypeAlreadySeen = typesSeen.containsKey(recType);
                  // if this is the first time we've seen this record type, infer and update this
                  // record type's schema, then save that schema back to the `typesSeen` map
                  if (!isTypeAlreadySeen && opType == OperationType.UPSERT) {
                    Map<String, DataTypeMapping> inferredSchema =
                        inferer.inferTypes(recordsForType);
                    Map<String, DataTypeMapping> finalSchema =
                        recordSink.createOrModifyRecordType(
                            recType, inferredSchema, recordsForType, primaryKey);
                    typesSeen.put(recType, finalSchema);
                  }

                  Map<String, DataTypeMapping> schema = typesSeen.get(recType);
                  // when updating relations only, do not update if there are no relations
                  if (recordSource.importMode() == BASE_ATTRIBUTES || !schema.isEmpty()) {
                    // For relations only, remove records that have no relations
                    var recordsToWrite =
                        recordSource.importMode() == RELATIONS
                            ? excludeEmptyRecords(recordsForType)
                            : recordsForType;

                    // write these records to the db, using the schema from the `typesSeen` map
                    try {
                      switch (opType) {
                        case UPSERT -> recordSink.upsertBatch(
                            recType, schema, recordsToWrite, primaryKey);
                        case DELETE -> recordSink.deleteBatch(recType, recordsToWrite);
                        default -> throw new BadStreamingWriteRequestException(
                            "Unsupported operation type: " + opType);
                      }
                      // update the result counts
                      result.increaseCount(recType, recordsToWrite.size());
                    } catch (IOException e) {
                      throw new BadStreamingWriteRequestException(e);
                    }
                  }
                });
      }
    } catch (IOException e) {
      throw new BadStreamingWriteRequestException(e);
    }
    return result;
  }

  private static List<Record> excludeEmptyRecords(List<Record> recordsForType) {
    return recordsForType.stream().filter(rec -> !rec.attributeSet().isEmpty()).toList();
  }
}
