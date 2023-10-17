package org.databiosphere.workspacedataservice.shared.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BearerTokenTest {

  @Test
  void nullIsNotValid() {
    assertFalse(BearerToken.isValid(null));
  }

  // empty string will cause 401s in practice, but BearerToken is only lightweight validation
  @Test
  void EmptyStringIsValid() {
    assertTrue(BearerToken.isValid(""));
  }

  // whitespace-only string will cause 401s in practice, but BearerToken is only lightweight
  // validation
  @Test
  void WhitespaceIsValid() {
    assertTrue(BearerToken.isValid(" "));
  }

  @Test
  void PopulatedStringIsValid() {
    assertTrue(BearerToken.isValid("Hello World"));
  }
}
