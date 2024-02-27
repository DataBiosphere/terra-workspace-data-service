package org.databiosphere.workspacedataservice.config;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Optional;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.lang.Nullable;

/**
 * ConfigurationProperties class, loaded as a member of TwdsProperties, representing the
 * `twds.instance` property hierarchy
 */
public class InstanceProperties {
  private boolean validWorkspaceId = false; // assume false until configured otherwise
  private boolean initializeCollectionOnStartup;
  private WorkspaceId workspaceId;
  @Nullable private WorkspaceId sourceWorkspaceId;

  /**
   * Returns the {@link WorkspaceId} for the environment $WORKSPACE_ID.
   *
   * @throws ConfigurationException if a valid workspaceId isn't present
   */
  public WorkspaceId workspaceId() {
    if (!validWorkspaceId) {
      throw new ConfigurationException("No workspaceId configured");
    }
    return workspaceId;
  }

  public boolean hasValidWorkspaceId() {
    return validWorkspaceId;
  }

  void setWorkspaceId(String workspaceId) {
    if (isNotBlank(workspaceId)) {
      try {
        this.workspaceId = WorkspaceId.fromString(workspaceId);
        validWorkspaceId = true;
      } catch (Exception e) {
        validWorkspaceId = false;
      }
    }
  }

  public Optional<WorkspaceId> sourceWorkspaceId() {
    return Optional.ofNullable(sourceWorkspaceId);
  }

  void setSourceWorkspaceId(String sourceWorkspaceId) {
    if (isNotBlank(sourceWorkspaceId)) {
      try {
        this.sourceWorkspaceId = WorkspaceId.fromString(sourceWorkspaceId);
      } catch (Exception e) {
        throw new ConfigurationException("Invalid sourceWorkspaceId: " + workspaceId, e);
      }
    }
  }

  void setInitializeCollectionOnStartup(boolean initializeCollectionOnStartup) {
    this.initializeCollectionOnStartup = initializeCollectionOnStartup;
  }

  public boolean getInitializeCollectionOnStartup() {
    return initializeCollectionOnStartup;
  }
}
