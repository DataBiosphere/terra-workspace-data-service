package org.databiosphere.workspacedataservice.shared.model;

import java.util.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

/** Spring Data-annotated model to represent a WDS Collection. */
@Table(schema = "sys_wds", name = "collection")
public class WdsCollection implements Persistable<CollectionId> {
  private final WorkspaceId workspaceId;

  @Id
  @Column("id")
  private final CollectionId collectionId;

  private final String name;
  private final String description;

  public WdsCollection(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    this.workspaceId = workspaceId;
    this.collectionId = collectionId;
    this.name = name;
    this.description = description;
  }

  /**
   * @return
   */
  @Override
  public CollectionId getId() {
    return collectionId;
  }

  public boolean isNew() {
    return false;
  }

  public WorkspaceId workspaceId() {
    return workspaceId;
  }

  @Id
  @Column("id")
  public CollectionId collectionId() {
    return collectionId;
  }

  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (WdsCollection) obj;
    return Objects.equals(this.workspaceId, that.workspaceId)
        && Objects.equals(this.collectionId, that.collectionId)
        && Objects.equals(this.name, that.name)
        && Objects.equals(this.description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workspaceId, collectionId, name, description);
  }

  @Override
  public String toString() {
    return "WdsCollection["
        + "workspaceId="
        + workspaceId
        + ", "
        + "collectionId="
        + collectionId
        + ", "
        + "name="
        + name
        + ", "
        + "description="
        + description
        + ']';
  }
}
