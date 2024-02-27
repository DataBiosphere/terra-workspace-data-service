package org.databiosphere.workspacedataservice.config;

import java.util.UUID;

/**
 * ConfigurationProperties class, loaded as a member of TwdsProperties, representing the
 * `twds.instance` property hierarchy
 */
public class InstanceProperties {

  private String workspaceId;
  private UUID workspaceUuid;
  private String sourceWorkspaceId;
  private UUID sourceWorkspaceUuid;

  public String getWorkspaceId() {
    return workspaceId;
  }

  /**
   * Will return null if workspace id is populated but is not a valid UUID
   *
   * @return the workspace id as a UUID
   */
  public UUID getWorkspaceUuid() {
    return workspaceUuid;
  }

  public void setWorkspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
    try {
      this.workspaceUuid = UUID.fromString(workspaceId);
    } catch (Exception e) {
      // noop; validation of the workspaceId is handled elsewhere. See StartupConfig.
    }
  }

  /**
   * Will return null if source workspace id is populated but is not a valid UUID
   *
   * @return the source workspace id as a UUID
   */
  public UUID getSourceWorkspaceUuid() {
    return sourceWorkspaceUuid;
  }

  public String getSourceWorkspaceId() {
    return sourceWorkspaceId;
  }

  public void setSourceWorkspaceId(String sourceWorkspaceId) {
    this.sourceWorkspaceId = sourceWorkspaceId;
    try {
      this.sourceWorkspaceUuid = UUID.fromString(sourceWorkspaceId);
    } catch (Exception e) {
      // noop; validation of the workspaceId is handled elsewhere. See StartupConfig.
    }
  }
}
