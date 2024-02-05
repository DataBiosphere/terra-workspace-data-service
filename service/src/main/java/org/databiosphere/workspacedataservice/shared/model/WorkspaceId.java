package org.databiosphere.workspacedataservice.shared.model;

import java.util.UUID;

/**
 * Model to represent a workspace id, which currently is a UUID. Since we use UUIDs throughout our
 * code for multiple use cases, this wrapper class exists to help disambiguate between those UUIDs.
 *
 * @param id the workspace's id
 */
public record WorkspaceId(UUID id) {

  // disallow nulls
  public WorkspaceId {
    if (id == null) {
      throw new IllegalArgumentException("Id cannot be null");
    }
  }

  /**
   * Create a new WorkspaceId using the given id
   *
   * @param id the workspace's id
   * @return new WorkspaceId
   */
  public static WorkspaceId of(UUID id) {
    return new WorkspaceId(id);
  }

  /**
   * Create a new WorkspaceId using the given id
   *
   * @param id the workspace's id
   * @return new WorkspaceId
   */
  public static WorkspaceId fromString(String id) {
    return WorkspaceId.of(UUID.fromString(id));
  }

  /**
   * Create a new WorkspaceId using the given id
   *
   * @par public static WorkspaceId fromString(String id) { return
   *     WorkspaceId.of(UUID.fromString(id)); }am id the workspace's id
   * @return new WorkspaceId
   */
  @Override
  public String toString() {
    return this.id().toString();
  }
}
