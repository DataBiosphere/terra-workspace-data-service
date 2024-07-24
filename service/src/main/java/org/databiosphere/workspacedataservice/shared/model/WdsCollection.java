package org.databiosphere.workspacedataservice.shared.model;

import java.util.Objects;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.InsertOnlyProperty;
import org.springframework.data.relational.core.mapping.Table;

/** Spring Data-annotated model to represent a WDS Collection. */
@Table(schema = "sys_wds", name = "collection")
public class WdsCollection implements Persistable<CollectionId> {

  @Id
  @Column("id")
  private final CollectionId collectionId;

  // for safety - workspaceId cannot be changed after insert
  @InsertOnlyProperty private final WorkspaceId workspaceId;

  private final String name;
  private final String description;

  public WdsCollection(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    this.workspaceId = workspaceId;
    this.collectionId = collectionId;
    this.name = name;
    this.description = description;
  }

  public static WdsCollection from(
      WorkspaceId workspaceId, CollectionServerModel collectionServerModel) {
    return new WdsCollection(
        workspaceId,
        CollectionId.of(collectionServerModel.getId()),
        collectionServerModel.getName(),
        collectionServerModel.getDescription());
  }

  /**
   * Required by Persistable.
   *
   * @return the collection id
   */
  @Override
  public CollectionId getId() {
    return collectionId;
  }

  /**
   * Required by Persistable.
   *
   * @return always false. See also WdsCollectionCreateRequest.
   */
  public boolean isNew() {
    return false;
  }

  public WorkspaceId workspaceId() {
    return workspaceId;
  }

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
