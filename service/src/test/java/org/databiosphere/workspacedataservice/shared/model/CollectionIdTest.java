package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class CollectionIdTest {

  @Test
  void disallowNulls() {
    assertThrows(IllegalArgumentException.class, () -> CollectionId.of(null));
  }

  @Test
  void disallowNullsStaticConstructor() {
    assertThrows(IllegalArgumentException.class, () -> new CollectionId(null));
  }

  // does .toString() work just like UUID.toString()?
  @Test
  void ValidIdToString() {
    UUID validId = UUID.randomUUID();
    assertEquals(validId.toString(), CollectionId.of(validId).toString());
  }

  // does the CollectionId.of() static constructor work just like the standard constructor?
  @Test
  void ofConstructor() {
    UUID validId = UUID.randomUUID();
    CollectionId constructed = new CollectionId(validId);
    CollectionId madeViaOf = CollectionId.of(validId);
    assertEquals(constructed.id(), madeViaOf.id());
    assertEquals(constructed, madeViaOf);
  }
}
