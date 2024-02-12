package org.databiosphere.workspacedataservice.config;

/**
 * ConfigurationProperties class, loaded as a member of TwdsProperties, representing the
 * `twds.instance` property hierarchy
 */
public class InstanceProperties {

  private String workspaceId;
  private String sourceWorkspaceId;

  public String getWorkspaceId() {
    return workspaceId;
  }

  public void setWorkspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
  }

  public String getSourceWorkspaceId() {
    return sourceWorkspaceId;
  }

  public void setSourceWorkspaceId(String sourceWorkspaceId) {
    this.sourceWorkspaceId = sourceWorkspaceId;
  }
}
