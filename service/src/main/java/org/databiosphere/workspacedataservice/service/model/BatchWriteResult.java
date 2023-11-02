package org.databiosphere.workspacedataservice.service.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public class BatchWriteResult {
  Map<RecordType, Integer> resultCounts;

  public BatchWriteResult(Map<RecordType, Integer> resultCounts) {
    this.resultCounts = resultCounts;
  }

  public static BatchWriteResult empty() {
    return new BatchWriteResult(new HashMap<>());
  }

  public int getUpdatedCount(RecordType recordType) {
    // TODO deal with missing keys?
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
