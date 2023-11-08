package org.databiosphere.workspacedataservice.service.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * The result of a batch-write operation that can update multiple record types. Provides methods for
 * getting and setting the number of records that were written, grouped by record type.
 */
public class BatchWriteResult {
  Map<RecordType, Integer> resultCounts;

  public BatchWriteResult(Map<RecordType, Integer> resultCounts) {
    this.resultCounts = resultCounts;
  }

  public static BatchWriteResult empty() {
    return new BatchWriteResult(new HashMap<>());
  }

  public int getUpdatedCount(RecordType recordType) {
    return resultCounts.get(recordType);
  }

  public void increaseCount(RecordType recordType, int count) {
    resultCounts.compute(recordType, (key, value) -> (value == null) ? count : value + count);
  }

  public void initialize(RecordType recordType) {
    resultCounts.putIfAbsent(recordType, 0);
  }

  public Set<Map.Entry<RecordType, Integer>> entrySet() {
    return resultCounts.entrySet();
  }
}
