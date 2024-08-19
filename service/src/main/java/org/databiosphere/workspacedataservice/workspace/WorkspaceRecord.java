package org.databiosphere.workspacedataservice.workspace;

import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

/** Spring Data-annotated model to represent a workspace. */
@Table(schema = "sys_wds", name = "workspace")
public class WorkspaceRecord implements Persistable<WorkspaceId> {

  @Id private final WorkspaceId workspaceId;
  private final WorkspaceDataTableType dataTableType;
  @Transient private final boolean newFlag;

  // Spring Data will use this constructor, which sets a default for the transient newFlag
  @PersistenceCreator
  public WorkspaceRecord(WorkspaceId workspaceId, WorkspaceDataTableType dataTableType) {
    this.workspaceId = workspaceId;
    this.dataTableType = dataTableType;
    this.newFlag = false;
  }

  public WorkspaceRecord(
      WorkspaceId workspaceId, WorkspaceDataTableType dataTableType, boolean newFlag) {
    this.workspaceId = workspaceId;
    this.dataTableType = dataTableType;
    this.newFlag = newFlag;
  }

  public WorkspaceId getWorkspaceId() {
    return workspaceId;
  }

  public WorkspaceDataTableType getDataTableType() {
    return dataTableType;
  }

  @Transient
  public boolean isNewFlag() {
    return newFlag;
  }

  /**
   * Required by {@link Persistable} interface; returns the id of this row.
   *
   * @return the id
   */
  @Override
  public WorkspaceId getId() {
    return getWorkspaceId();
  }

  /**
   * Required by {@link Persistable} interface; determines if this row is an insert or an update
   *
   * @return true if an insert; false if update
   */
  @Override
  public boolean isNew() {
    return isNewFlag();
  }
}
