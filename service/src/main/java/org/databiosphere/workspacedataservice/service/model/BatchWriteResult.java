package org.databiosphere.workspacedataservice.service.model;

import com.google.common.base.Preconditions;
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

  public Integer getUpdatedCount(RecordType recordType) {
    return resultCounts.get(recordType);
  }

  public Integer getTotalUpdatedCount() {
    return resultCounts.values().stream().reduce(0, Integer::sum);
  }

  public void merge(BatchWriteResult other) {
    other.resultCounts.forEach((key, value) -> this.resultCounts.merge(key, value, Integer::sum));
  }

  public void increaseCount(RecordType recordType, int count) {
    Preconditions.checkArgument(count >= 0, "Count cannot be negative");
    resultCounts.compute(recordType, (key, value) -> (value == null) ? count : value + count);
  }

  public void initialize(RecordType recordType) {
    resultCounts.putIfAbsent(recordType, 0);
  }

  public Set<Map.Entry<RecordType, Integer>> entrySet() {
    return resultCounts.entrySet();
  }
}
