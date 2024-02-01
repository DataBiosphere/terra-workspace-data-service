package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class InstanceIdTest {

  @Test
  void disallowNulls() {
    assertThrows(IllegalArgumentException.class, () -> InstanceId.of(null));
  }

  @Test
  void disallowNullsStaticConstructor() {
    assertThrows(IllegalArgumentException.class, () -> new InstanceId(null));
  }

  // does .toString() work just like UUID.toString()?
  @Test
  void ValidIdToString() {
    UUID validId = UUID.randomUUID();
    assertEquals(validId.toString(), InstanceId.of(validId).toString());
  }

  // does the InstanceId.of() static constructor work just like the standard constructor?
  @Test
  void ofConstructor() {
    UUID validId = UUID.randomUUID();
    InstanceId constructed = new InstanceId(validId);
    InstanceId madeViaOf = InstanceId.of(validId);
    assertEquals(constructed.id(), madeViaOf.id());
    assertEquals(constructed, madeViaOf);
  }
}
