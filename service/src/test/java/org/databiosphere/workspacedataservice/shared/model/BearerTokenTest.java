package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BearerTokenTest {

  @Test
  void nullIsEmpty() {
    assertTrue(BearerToken.ofNullable(null).isEmpty());
  }

  // empty string will cause 401s in practice, but BearerToken is only lightweight validation
  @Test
  void emptyStringIsNotEmpty() {
    assertTrue(BearerToken.of("").nonEmpty());
  }

  // whitespace-only string will cause 401s in practice, but BearerToken is only lightweight
  // validation
  @Test
  void whitespaceIsNotEmpty() {
    assertTrue(BearerToken.of(" ").nonEmpty());
  }

  @Test
  void populatedStringIsNotEmpty() {
    assertTrue(BearerToken.of("Hello World").nonEmpty());
  }

  @Test
  void emptyConstructorIsEmpty() {
    assertTrue(BearerToken.empty().isEmpty());
  }

  @Test
  void emptyConstructorNonEmptyIsFalse() {
    assertFalse(BearerToken.empty().nonEmpty());
  }
}
