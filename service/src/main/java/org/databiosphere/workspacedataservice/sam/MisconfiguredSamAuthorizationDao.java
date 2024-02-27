package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will be used instead of HttpSamAuthorizationDao if a well-formed WORKSPACE_ID env var
 * is not provided.
 *
 * <p>This class will fail all permission checks and log a warning each time. Its objective is to
 * disallow any write operations that require permissions, while still allowing WDS to start up.
 */
public class MisconfiguredSamAuthorizationDao implements SamAuthorizationDao {

  private final String errorMessage;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public MisconfiguredSamAuthorizationDao(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public boolean hasCreateCollectionPermission() {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasCreateCollectionPermission(BearerToken token) {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasDeleteCollectionPermission() {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasDeleteCollectionPermission(BearerToken token) {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasWriteWorkspacePermission() {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasWriteWorkspacePermission(WorkspaceId workspaceId) {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId) {
    return logWarningAndDenyAccess();
  }

  @Override
  public boolean hasReadWorkspacePermission(WorkspaceId workspaceId, BearerToken token) {
    return logWarningAndDenyAccess();
  }

  private boolean logWarningAndDenyAccess() {
    logger.warn("Sam permission check failed. {}", errorMessage);
    return false;
  }
}
