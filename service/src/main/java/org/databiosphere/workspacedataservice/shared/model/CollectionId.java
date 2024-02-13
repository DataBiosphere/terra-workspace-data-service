package org.databiosphere.workspacedataservice.shared.model;

import java.util.UUID;

/**
 * Model to represent a collection id, which currently is a UUID. Since we use UUIDs throughout our
 * code for multiple use cases, this wrapper class exists to help disambiguate between those UUIDs.
 *
 * @param id the collection's id
 */
public record CollectionId(UUID id) {

  // disallow nulls
  public CollectionId {
    if (id == null) {
      throw new IllegalArgumentException("Id cannot be null");
    }
  }

  /**
   * Create a new CollectionId using the given id
   *
   * @param id the collection's id
   * @return new CollectionId
   */
  public static CollectionId of(UUID id) {
    return new CollectionId(id);
  }

  /**
   * Create a new CollectionId using the given id
   *
   * @param id the collection's id
   * @return new CollectionId
   */
  public static CollectionId fromString(String id) {
    return CollectionId.of(UUID.fromString(id));
  }

  @Override
  public String toString() {
    return this.id().toString();
  }
}
