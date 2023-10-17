package org.databiosphere.workspacedataservice.shared.model;

/**
 * Record class to represent an auth token; use this instead of String for more type safety and
 * readability of argument lists. As of this writing, WDS includes both Nimbus and Apache Oltu as
 * transitive dependencies, and both of those libraries include models to represent a token; but
 * they also both have more complexity than we need, and we don't want to depend too much on
 * transitive dependencies
 *
 * @param value the String representation of the token
 */
public record BearerToken(String value) {

  /**
   * Validates that a given String is a valid access token. For the sake of WDS's validation, all we
   * care about is that the String is non-null; we do not call out to an OAuth provider to validate
   * expiration or authenticity.
   *
   * @param input the value to validate
   * @return true if the value is valid
   */
  public static boolean isValid(String input) {
    return input != null;
  }
}
