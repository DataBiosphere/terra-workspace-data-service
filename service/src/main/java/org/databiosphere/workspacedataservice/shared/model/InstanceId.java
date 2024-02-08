package org.databiosphere.workspacedataservice.shared.model;

import java.util.UUID;

/**
 * Model to represent an instance id, which currently is a UUID. Since we use UUIDs throughout our
 * code for multiple use cases, this wrapper class exists to help disambiguate between those UUIDs.
 *
 * @param id the instance's id
 */
public record InstanceId(UUID id) {

  // disallow nulls
  public InstanceId {
    if (id == null) {
      throw new IllegalArgumentException("Id cannot be null");
    }
  }

  /**
   * Create a new InstanceId using the given id
   *
   * @param id the instance's id
   * @return new InstanceId
   */
  public static InstanceId of(UUID id) {
    return new InstanceId(id);
  }

  /**
   * Create a new InstanceId using the given id
   *
   * @param id the instance's id
   * @return new InstanceId
   */
  public static InstanceId fromString(String id) {
    return InstanceId.of(UUID.fromString(id));
  }

  @Override
  public String toString() {
    return this.id().toString();
  }
}
