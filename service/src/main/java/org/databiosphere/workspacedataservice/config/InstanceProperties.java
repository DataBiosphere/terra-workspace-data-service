package org.databiosphere.workspacedataservice.config;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.lang.Nullable;

/**
 * ConfigurationProperties class, loaded as a member of TwdsProperties, representing the
 * `twds.instance` property hierarchy
 */
public class InstanceProperties {
  /**
   * Shorthand annotation for @Qualifier("singleTenant"), used to mark the {@link WorkspaceId} bean
   * used when running in single-tenant mode.
   */
  @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
  @Retention(RetentionPolicy.RUNTIME)
  @Qualifier("singleTenant")
  public @interface SingleTenant {}

  private boolean validWorkspaceId;
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
    if (!isNullOrEmpty(workspaceId)) {
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
    if (!isNullOrEmpty(sourceWorkspaceId)) {
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
