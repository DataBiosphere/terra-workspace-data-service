package org.databiosphere.workspacedataservice.service.model;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;

class BatchWriteResultTest {

  @Test
  void emptyConstructor() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();
    assertThat(batchWriteResult.entrySet()).isEmpty();
  }

  @Test
  void getMissingRecordType() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();
    batchWriteResult.initialize(RecordType.valueOf("thing"));
    batchWriteResult.increaseCount(RecordType.valueOf("thing"), 42);
    // "whatever" does not exist in this result, should return null
    Integer actual = batchWriteResult.getUpdatedCount(RecordType.valueOf("whatever"));
    assertNull(actual);
  }

  @Test
  void incrementMissingRecordType() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();
    batchWriteResult.initialize(RecordType.valueOf("thing"));
    batchWriteResult.increaseCount(RecordType.valueOf("thing"), 42);
    // "whatever" does not exist in this result, but "increaseCount" will add it
    batchWriteResult.increaseCount(RecordType.valueOf("whatever"), 123);
    Integer actual = batchWriteResult.getUpdatedCount(RecordType.valueOf("whatever"));
    assertEquals(123, actual);
  }

  @Test
  void incrementMultipleTimes() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();

    batchWriteResult.initialize(RecordType.valueOf("multi"));
    assertEquals(0, batchWriteResult.getUpdatedCount(RecordType.valueOf("multi")));

    batchWriteResult.increaseCount(RecordType.valueOf("multi"), 12);
    assertEquals(12, batchWriteResult.getUpdatedCount(RecordType.valueOf("multi")));

    batchWriteResult.increaseCount(RecordType.valueOf("multi"), 24);
    assertEquals(36, batchWriteResult.getUpdatedCount(RecordType.valueOf("multi")));

    batchWriteResult.increaseCount(RecordType.valueOf("multi"), 1);
    assertEquals(37, batchWriteResult.getUpdatedCount(RecordType.valueOf("multi")));
  }

  @Test
  void incrementNegative() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();

    RecordType recordType = RecordType.valueOf("negative");

    batchWriteResult.initialize(recordType);
    assertEquals(0, batchWriteResult.getUpdatedCount(recordType));

    Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> batchWriteResult.increaseCount(recordType, -123));

    assertEquals("Count cannot be negative", e.getMessage());
  }

  @Test
  void multipleTypes() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();
    batchWriteResult.increaseCount(RecordType.valueOf("first"), 1);
    batchWriteResult.increaseCount(RecordType.valueOf("second"), 2);
    batchWriteResult.increaseCount(RecordType.valueOf("third"), 3);

    // verify counts
    assertEquals(1, batchWriteResult.getUpdatedCount(RecordType.valueOf("first")));
    assertEquals(2, batchWriteResult.getUpdatedCount(RecordType.valueOf("second")));
    assertEquals(3, batchWriteResult.getUpdatedCount(RecordType.valueOf("third")));

    // verify set of record types
    assertEquals(
        Set.of(
            RecordType.valueOf("first"), RecordType.valueOf("second"), RecordType.valueOf("third")),
        batchWriteResult.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet()));

    // increment "first" again
    batchWriteResult.increaseCount(RecordType.valueOf("first"), 10);

    // verify counts
    assertEquals(11, batchWriteResult.getUpdatedCount(RecordType.valueOf("first")));
    assertEquals(2, batchWriteResult.getUpdatedCount(RecordType.valueOf("second")));
    assertEquals(3, batchWriteResult.getUpdatedCount(RecordType.valueOf("third")));

    // verify set of record types
    assertEquals(
        Set.of(
            RecordType.valueOf("first"), RecordType.valueOf("second"), RecordType.valueOf("third")),
        batchWriteResult.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
  }

  @Test
  void mergeTwoResults() {
    BatchWriteResult batchWriteResultOne = BatchWriteResult.empty();
    batchWriteResultOne.increaseCount(RecordType.valueOf("first"), 1);
    batchWriteResultOne.increaseCount(RecordType.valueOf("second"), 2);
    batchWriteResultOne.increaseCount(RecordType.valueOf("third"), 3);

    BatchWriteResult batchWriteResultTwo = BatchWriteResult.empty();
    batchWriteResultTwo.increaseCount(RecordType.valueOf("fourth"), 4);
    batchWriteResultTwo.increaseCount(RecordType.valueOf("fifth"), 5);
    batchWriteResultTwo.increaseCount(RecordType.valueOf("sixth"), 6);

    batchWriteResultOne.merge(batchWriteResultTwo);

    // verify counts
    assertEquals(1, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("first")));
    assertEquals(2, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("second")));
    assertEquals(3, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("third")));
    assertEquals(4, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("fourth")));
    assertEquals(5, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("fifth")));
    assertEquals(6, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("sixth")));

    BatchWriteResult batchWriteResultThree = BatchWriteResult.empty();
    batchWriteResultThree.increaseCount(RecordType.valueOf("fourth"), 4);

    batchWriteResultOne.merge(batchWriteResultThree);
    assertEquals(8, batchWriteResultOne.getUpdatedCount(RecordType.valueOf("fourth")));
  }

  @Test
  void calculateTotalCount() {
    BatchWriteResult batchWriteResult = BatchWriteResult.empty();
    batchWriteResult.increaseCount(RecordType.valueOf("first"), 1);
    batchWriteResult.increaseCount(RecordType.valueOf("second"), 2);
    batchWriteResult.increaseCount(RecordType.valueOf("third"), 3);

    assertEquals(6, batchWriteResult.getTotalUpdatedCount());
  }
}
