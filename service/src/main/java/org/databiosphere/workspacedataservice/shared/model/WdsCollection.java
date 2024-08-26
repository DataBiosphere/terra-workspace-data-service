package org.databiosphere.workspacedataservice.shared.model;

import java.util.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
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

  // newFlag is not saved to the db; it is used to differentiate db inserts and updates.
  // see the isNew() method below.
  @Transient private final boolean newFlag;

  // Spring Data will use this constructor, which sets a default for the transient newFlag
  @PersistenceCreator
  public WdsCollection(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    this.workspaceId = workspaceId;
    this.collectionId = collectionId;
    this.name = name;
    this.description = description;
    this.newFlag = false;
  }

  public WdsCollection(
      WorkspaceId workspaceId,
      CollectionId collectionId,
      String name,
      String description,
      boolean newFlag) {
    this.workspaceId = workspaceId;
    this.collectionId = collectionId;
    this.name = name;
    this.description = description;
    this.newFlag = newFlag;
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
   * Required by {@link Persistable} interface; determines if this row is an insert or an update.
   *
   * <p>Spring Data, when writing this object to the database, will call this isNew() method. When
   * isNew() returns true, Spring Data will insert this object as a new row. Else, Spring Data will
   * update an existing row with this object.
   *
   * @see <a
   *     href="https://docs.spring.io/spring-data/relational/reference/repositories/core-concepts.html#is-new-state-detection">Spring
   *     Data doc</a>
   * @return true if an insert; false if update
   */
  public boolean isNew() {
    return isNewFlag();
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

  @Transient
  public boolean isNewFlag() {
    return newFlag;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (WdsCollection) obj;
    return Objects.equals(this.workspaceId, that.workspaceId)
        && Objects.equals(this.collectionId, that.collectionId)
        && Objects.equals(this.name, that.name)
        && Objects.equals(this.description, that.description)
        && Objects.equals(this.newFlag, that.newFlag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workspaceId, collectionId, name, description, newFlag);
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
        + ", "
        + "newFlag="
        + newFlag
        + ']';
  }
}
